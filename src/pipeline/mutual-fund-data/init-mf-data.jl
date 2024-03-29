using Revise
using DataFrames
using CSV
using Arrow
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

SKIP_FILES = ["info.csv", "equity_allocation.csv", "non_us_equity_allocation.csv"]

function init_mf_data()
    task_start = time()
    mf_data_collection = _read_mf_data()
    
    mf_data = reduce(
        (l,r)->outerjoin(l,r; on=[:fundid, :secid, :date]),
        mf_data_collection
    )

    ###
    x = mf_data_collection[2]
    x[x.fundid .== "FSUSA003JQ" .&& x.date .== Date(2022,11,1), :]
    gb = groupby(mf_data, [:fundid, :date])
    fund_init_gret_mean = combine(
        gb,
        :gross_returns => mean => :non_missing_fund_gret
    )

    count(!ismissing, fund_init_gret_mean.non_missing_fund_gret)

    raw_gret = CSV.read(joinpath(DIRS.mf.raw, "gross_returns.csv"), DataFrame)

    gb = groupby(raw_gret, :FundId)
    fund_raw_gret_mean = combine(
        gb,
        propertynames(raw_gret)[4:end] .=> mean
    )

    unique_raw = Set(round.(fund_raw_gret_mean[:, 2:end] |> Matrix,digits=4))

    unique_init = Set(round.(fund_init_gret_mean.non_missing_fund_gret, digits=4))

    setdiff(unique_raw, unique_init)

    for i in unique_init
        if !(i in unique_raw)
            error(i)
        end
    end

    9.4965 in unique_init

    fund_init_gret_mean[coalesce.(round.(fund_init_gret_mean.non_missing_fund_gret,digits=4) .== 13.6586,false), :]

    rename!(fund_raw_gret_mean, [:fundid, Symbol.([Date(1990,1,1) + Month(i) for i in 0:size(fund_raw_gret_mean, 2)-2])...])

    fund_raw_gret_mean[fund_raw_gret_mean.fundid .== "FSUSA003JQ", Symbol(Date(2022,11,1))]

    raw_gret[raw_gret.FundId .== "FSUSA003JQ", r"Name|FundId|SecId|.*2022-11.*"]
    mf_data[mf_data.fundid .== "FSUSA003JQ" .&& mf_data.date .== Date(2022,11,1), :]

    drop_allmissing!(mf_data, Not([:fundid, :secid, :date]); dims=:rows)

    printtime("initialising mutual fund data", task_start, minutes=false)
    return mf_data
end

function _read_mf_data()
    folder = DIRS.mf.raw
    files = readdir(folder, )
    data = DataFrame[]
    for file in files # file = "gross_returns.csv"
        file in SKIP_FILES && continue

        filepath = joinpath(folder, file)
        fieldname = splitext(file)[1]
        
        data_part = init_raw(filepath)
        data_part_melt = stack(
            data_part, Not([:name, :fundid, :secid]), [:fundid, :secid],
            variable_name=:date, value_name=fieldname
        )

        data_part_melt.date = Date.(data_part_melt.date)

        push!(data, data_part_melt)
    end
    return data
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = init_mf_data()
    output_filename = makepath(DIRS.mf.init, "mf-data.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing mutual fund data", task_start, minutes=false)
end