using Revise
using DataFrames
using Arrow
using Dates
using DataStructures
using CategoricalArrays
using Base.Threads

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function init_mf_data()
    task_start = time()
    mf_data = _read_mf_timeseries(task_start)
    
    # Retype, rename, and reorder the columns, and sort the rows
    _init_data!(mf_data)

    mf_info = _read_mf_crosssection(task_start)

    # Retype and rename the columns
    _init_info!(mf_info)

    save_filename_data = makepath(DIRS.mf.init, "mf-data.arrow")
    save_filename_info = makepath(DIRS.mf.init, "mf-info.arrow")
    Arrow.write(save_filename_data, mf_data)
    Arrow.write(save_filename_info, mf_info)

    printtime("initialising mutual fund data", task_start, minutes=false)
    return
end

function _read_mf_timeseries(task_start)
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

    # TODO: Make type adjustments here to save on memory

    printtime(
        "reading mutual fund timeseries data", task_start;
        process_start_time=process_start, minutes=false
    )

    mf_ret.caldt = passmissing(lastdayofmonth, mf_ret.caldt)
    mf_tna.caldt = passmissing(lastdayofmonth, mf_tna.caldt)

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

function _decompress_timeseries(short_data)
    col_names = propertynames(short_data)
    data_cols = propertynames(short_data[!, Not(:crsp_fundno, :begdt, :enddt)])

    if any(ismissing.(short_data.begdt) .⊻ ismissing.(short_data.enddt))
        error("Some rows have a single one of beginning or ending date")
    end
    
    short_data = dropmissing(short_data, [:begdt, :enddt])
    sort!(short_data, [:crsp_fundno, :begdt])

    # Align dates with days of 15 or lower to the end of the previous month, else
    # to the end of the current month
    align(date) = (day(date) .<= 15 ? lastdayofmonth(date-Month(1)) : lastdayofmonth(date))
    short_data.begdt = align.(short_data.begdt)
    short_data.enddt = align.(short_data.enddt)

    short_data.date_domain = [
        # Beginning date is exclusive
        row.begdt+Month(1):Month(1):row.enddt for row in eachrow(short_data)
    ]
    short_data.span_length = length.(short_data.date_domain)

    total_rows = sum(short_data.span_length)
    long_data = DataFrame()

    for col in col_names
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
            
            long_data[idx_range, :caldt] = lastdayofmonth.(period.date_domain)
            long_data[idx_range, data_cols] = repeat(DataFrame(period[data_cols]), n_rows)

            start_idx += n_rows
        end
    end

    return long_data
end

function _init_data!(mf_data)
    mf_data.crsp_fundno = convert.(Int, mf_data.crsp_fundno)
    mf_data.crsp_cl_grp = convert.(Union{Missing,Int}, mf_data.crsp_cl_grp)
    mf_data.crsp_obj_cd = CategoricalArray(mf_data.crsp_obj_cd)
    mf_data.index_fund_flag = CategoricalArray(mf_data.index_fund_flag)
    mf_data.et_flag = CategoricalArray(mf_data.et_flag)
    mf_data.retail_fund = coalesce.(mf_data.retail_fund .== "Y", missing)
    
    select!(mf_data, :crsp_cl_grp, :crsp_fundno, Not(:crsp_fundno, :crsp_cl_grp))

    rename!(
        mf_data,
        :crsp_fundno => :fund_class_id,
        :crsp_cl_grp => :class_group_id,
        :caldt => :date,
        :mret => :ret,
        :mtna => :net_assets,
        :exp_ratio => :costs,
        :crsp_obj_cd => :investment_objective
    )

    sort!(mf_data, [:fund_class_id, :date])
    return
end

function _read_mf_crosssection(task_start)
    process_start = time()
    mf_info = loadarrow(joinpath(DIRS.mf.raw, "fund_hdr.arrow"))
    select!(
        mf_info,
        [
            :crsp_fundno,
            :fund_name,
            :first_offer_dt,
            :end_dt,
            :delist_cd,
            :merge_fundno
        ]
    )

    printtime(
        "reading mutual info data", task_start;
        process_start_time=process_start, minutes=false
    )

    return mf_info
end

function _init_info!(mf_info)
    mf_info.crsp_fundno = convert.(Union{Missing,Int}, mf_info.crsp_fundno)
    mf_info.first_offer_dt = lastdayofmonth.(mf_info.first_offer_dt)
    mf_info.end_dt = lastdayofmonth.(mf_info.end_dt)
    mf_info.delist_cd = CategoricalArray(mf_info.delist_cd)
    mf_info.merge_fundno = convert.(Union{Missing,Int}, mf_info.merge_fundno)

    rename!(
        mf_info,
        :crsp_fundno => :fund_class_id,
        :first_offer_dt => :inception_date,
        :end_dt => :end_date,
        :delist_cd => :delist_code,
        :merge_fundno => :merge_fund_class_id
    )
    return
end

if abspath(PROGRAM_FILE) == @__FILE__
    init_mf_data()
end