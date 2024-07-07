using Revise
using DataFrames
using Arrow
using Dates
using DataStructures
using Base.Threads

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function init_mf_data()
    task_start = time()
    mf_data = _read_mf_timeseries()

    mf_data.crsp_fundno = Int.(mf_data.crsp_fundno)


    _drop_allmissing_funds!(mf_data, Not([:fundid, :secid, :date]); dims=:rows)
    
    mf_info = _read_mf_crosssection()

    printtime("initialising mutual fund data", task_start, minutes=false)
    return mf_data
end

function _read_mf_timeseries()
    process_start = time()
    mf_ret = loadarrow(joinpath(DIRS.mf.raw, "monthly_returns.arrow"))
    mf_tna = loadarrow(joinpath(DIRS.mf.raw, "monthly_tna.arrow"))
    
    compressed_timeseries = OrderedDict()
    compressed_timeseries[:mf_fees] = (
        loadarrow(joinpath(DIRS.mf.raw, "fund_fees.arrow"))
    )
    select!(
        compressed_timeseries[:mf_fees],
        [:crsp_fundno, :begdt, :enddt, :exp_ratio]
    )
    compressed_timeseries[:mf_frontload] = (
        loadarrow(joinpath(DIRS.mf.raw, "front_load.arrow"))
    )
    select!(
        compressed_timeseries[:mf_frontload],
        [:crsp_fundno, :begdt, :enddt, :front_load]
    )
    compressed_timeseries[:mf_rearload] = (
        loadarrow(joinpath(DIRS.mf.raw, "rear_load.arrow"))
    )
    select!(
        compressed_timeseries[:mf_rearload],
        [:crsp_fundno, :begdt, :enddt, :time_period, :rear_load]
    )
    compressed_timeseries[:mf_style] = (
        loadarrow(joinpath(DIRS.mf.raw, "fund_style.arrow"))
    )
    select!(
        compressed_timeseries[:mf_style],
        [:crsp_fundno, :begdt, :enddt, :crsp_obj_cd]
    )
    compressed_timeseries[:mf_info_timeseries] = (
        loadarrow(joinpath(DIRS.mf.raw, "fund_hdr_hist.arrow"))
    )
    select!(
        compressed_timeseries[:mf_info_timeseries],
        [
            :crsp_fundno,
            :chgdt,
            :chgenddt,
            :crsp_cl_grp,
            :index_fund_flag,
            :et_flag,
            :retail_fund
        ]
    )
    rename!(
        # TODO: Rushed it, handle this better
        compressed_timeseries[:mf_info_timeseries],
        :chgdt => :begdt,
        :chgenddt => :enddt
    )

    printtime(
        "reading mutual fund timeseries data", task_start;
        process_start_time=process_start, minutes=false
    )

    uncompressed_timeseries = []
    @threads for df_key in collect(keys(compressed_timeseries))
        process_start = time()
        push!(
            uncompressed_timeseries,
            _decompress_timeseries(compressed_timeseries[df_key])
        )

        printtime(
            "decompressing timeseries of $df_key", task_start;
            process_start_time=process_start, minutes=false
        )
    end

    data = reduce(
        (x, y) -> outerjoin(x, y, on=[:crsp_fundno, :caldt]),
        [mf_ret, mf_tna, uncompressed_timeseries...]
    )

    return data
end

function _decompress_timeseries(short_data_in)

    data_cols = propertynames(short_data_in[!, Not(:crsp_fundno, :begdt, :enddt)])

    short_data = dropmissing(short_data_in, [:begdt, :enddt]) # TODO: Rushed it, handle this better

    short_data.date_domain = [row.begdt:Month(1):row.enddt for row in eachrow(short_data)]
    short_data.span_length = length.(short_data.date_domain)

    total_rows = sum(short_data.span_length)
    long_data = DataFrame()

    for col in propertynames(short_data_in)
        coltype = eltype(short_data[!, col])
        col = (col == :begdt ? :caldt : col)
        col == :enddt && continue

        long_data[:, col] = Vector{Union{Missing, coltype}}(missing, total_rows)
    end

    start_idx = 1
    for (fundno, fund_group) in pairs(groupby(short_data, :crsp_fundno))

        for period in eachrow(fund_group)
            n_rows = period.span_length
            idx_range = start_idx:start_idx+n_rows-1

            try
                long_data[idx_range, :crsp_fundno] .= period.crsp_fundno
            catch e
                println("Error at fundno: $fundno, period: $period, idx_range: $idx_range")
                rethrow(e)
            end
            
            long_data[idx_range, :caldt] = period.date_domain
            long_data[idx_range, data_cols] = repeat(DataFrame(period[data_cols]), n_rows)

            start_idx += n_rows
        end
    end

    return long_data
end

function _drop_allmissing_funds!(data)
    fund_level_missing_mask = combine(
        groupby(data, :fundid),
        [
            col => (x->all(ismissing, x)) => col*"_mask"
            for col in names(data[:, Not(:fundid, :secid, :date)])
        ]...
    )

    all_missing_funds = fund_level_missing_mask[
        all.(eachrow(fund_level_missing_mask[:, Not(:fundid)])), :fundid
    ]

    delete!(data, findall(in(all_missing_funds), data.fundid))
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = init_mf_data()
    output_filename = makepath(DIRS.mf.init, "mf-data.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing mutual fund data", task_start, minutes=false)
end