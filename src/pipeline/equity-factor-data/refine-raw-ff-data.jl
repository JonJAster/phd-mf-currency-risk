using Revise
using DataFrames
using Arrow
using CSV
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function refine_raw_ff_data()
    task_start = time()

    ff_usa = _read_ff_raw("ff-usa.csv"; read_start=4, read_end=731)
    ff_usa_wml = _read_ff_raw("ff-usa-wml.csv"; read_start=14, read_end=1179)
    ff_dev = _read_ff_raw("ff-dev.csv"; read_start=7, read_end=411)
    ff_dev_wml = _read_ff_raw("ff-dev-wml.csv"; read_start=7, read_end=407)

    raw_data = CSV.read(joinpath(DIRS.eq.raw, "ff-raw.csv"), DataFrame; dateformat="yyyy-mm-dd")
    _init_ff_data!(raw_data)

    ff_data = _filter_to_desired_factors(raw_data, EQUITY_FF_FACTORS)

    printtime("refining raw FF data", task_start, minutes=false)
    return ff_data
end

function _read_ff_raw(filename; read_start, read_end) # filename = "ff-usa.csv"; read_start = 4; read_end = 731
    filepath = joinpath(DIRS.eq.raw, filename)

    file_height = countlines(filepath)
    footerskip = file_height - read_end

    raw_data = CSV.File(
        filepath;
        header=read_start,
        footerskip=footerskip,
        dateformat="yyyymm",
        types=Dict(:Column1=>Date)
    ) |> DataFrame

    series_names = names(raw_data)[2:end] |> _normalise_names .|> Symbol
    new_names = [:date; series_names]

    rename!(raw_data, new_names)

    raw_data[!, Not(:date)] ./= 100

    ###
    compare_rf = CSV.read(joinpath(DIRS.eq.raw, "jkp-country-mkt.csv"), DataFrame; dateformat="yyyy-mm-dd")
    compare_rf.rf = round.(compare_rf.mkt_vw .- compare_rf.mkt_vw_exc, digits= 4)

    dated_compare = compare_rf[firstdayofmonth.(compare_rf.eom) .∈ Ref(raw_data.date) .&& compare_rf.excntry .== "USA", :]
    dated_raw = raw_data[raw_data.date .∈ Ref(firstdayofmonth.(compare_rf.eom)), :]

    dated_compare.date = firstdayofmonth.(dated_compare.eom)

    compare_rf = innerjoin(
        dated_raw[!, [:date, :rf]], dated_compare[!, [:date, :rf]];
        on=:date, renamecols="_raw"=>"_compare"
    )

    modern_compare_rf = compare_rf[compare_rf.date .> Date(1990, 1, 1), :]

    cor(modern_compare_rf.rf_raw, modern_compare_rf.rf_compare)

    plot(modern_compare_rf.rf_raw, modern_compare_rf.rf_compare, seriestype=:scatter, legend=false, title="Raw vs. Compare RF", xlabel="Raw RF", ylabel="Compare RF")

    cor(dated_raw.rf, dated_compare.rf)
    plot(dated_raw.rf, dated_compare.rf, seriestype=:scatter, legend=false, title="Raw vs. Compare RF", xlabel="Raw RF", ylabel="Compare RF")

    plot(dated_raw.date, dated_raw.rf, seriestype=:line, title="Raw vs Compare", ylabel="Date", xlabel="RF", label="Raw")
    plot!(firstdayofmonth.(dated_compare.eom), dated_compare.rf, seriestype=:line, label="Compare")
    ###
end

function _normalise_names(names)
    relabel_excess(name) = replace(name, "-rf" => "_exc")
    normal_names = names .|> lowercase .|> relabel_excess
    return normal_names
end

if abspath(PROGRAM_FILE) == @__FILE__
    refine_raw_ff_data()
end