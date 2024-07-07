using Revise
using DataFrames
using Arrow
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function init_mf_data()
    task_start = time()
    mf_data = _read_mf_timeseries()
    mf_info = _read_mf_crosssection()

    _drop_allmissing_funds!(mf_data, Not([:fundid, :secid, :date]); dims=:rows)

    printtime("initialising mutual fund data", task_start, minutes=false)
    return mf_data
end

function _read_mf_timeseries()
    
    mf_ret = loadarrow(joinpath(DIRS.mf.raw, "monthly_returns.arrow"))
    mf_tna = loadarrow(joinpath(DIRS.mf.raw, "monthly_tna.arrow"))
    
    mf_fees = loadarrow(joinpath(DIRS.mf.raw, "fund_fees.arrow"))
    select!(mf_fees, [:crsp_fundno, :begdt, :enddt, :exp_ratio])
    mf_frontload = loadarrow(joinpath(DIRS.mf.raw, "front_load.arrow"))
    select!(mf_frontload, [:crsp_fundno, :begdt, :enddt, :front_load])
    mf_rearload = loadarrow(joinpath(DIRS.mf.raw, "rear_load.arrow"))
    select!(mf_rearload, [:crsp_fundno, :begdt, :enddt, :time_period, :rear_load])
    mf_style = loadarrow(joinpath(DIRS.mf.raw, "fund_style.arrow"))
    select!(mf_style, [:crsp_fundno, :begdt, :enddt, :crsp_obj_cd])
    mf_info_timeseries = loadarrow(joinpath(DIRS.mf.raw, "fund_hdr_hist.arrow"))
    select!(
        mf_info_timeseries,
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

    mf_fees_long = _decompress_timeseries(mf_fees)
    
    return data
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