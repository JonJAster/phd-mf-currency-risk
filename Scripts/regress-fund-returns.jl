using DataFrames
using CSV
using GLM
using Dates
using Base.Threads

include("CommonFunctions.jl")
include("CommonConstants.jl")
include("DataReader.jl")
using .CommonFunctions
using .CommonConstants
using .DataReader

<<<<<<< HEAD
const OUTPUT_FILESTRING_BASE = "./data/results/"
=======
const INPUT_FILESTRING_BASE_FUNDS = "./data/transformed/mutual-funds"
const INPUT_FILESTRING_CURRENCY_FACTORS = "./data/prepared/currencies/currency_factors.csv"
const INPUT_FILESTRING_LONGSHORT_FACTORS = "./data/prepared/equities/equity_factors.csv"
const INPUT_FILESTRING_MARKET = "./data/transformed/equities/global_MKT_gross.csv"
const INPUT_FILESTRING_RISKFREE = "./data/transformed/equities/riskfree.csv"
const INPUT_FILESTRING_CURRENCY_MAP = "./data/raw/currency_to_country.csv"
const READ_COLUMNS_FUNDS = [:fundid, :date, :ret_gross_m, :domicile]
const OUTPUT_FILESTRING_BASE = ".data/results/"

const BENCHMARK_MODELS = Dict(
    :world_capm => [:MKT],
    :world_ff3 => [:MKT, :SMB, :HML],
    :world_ff5 => [:MKT, :SMB, :HML, :RMW, :CMA],
    :world_ffcarhart => [:MKT, :SMB, :HML, :WML],
    :world_ff6 => [:MKT, :SMB, :HML, :RMW, :CMA, :WML]
)
const CURRENCYRISK_MODELS = Dict(
    :lrv => [:hml_fx, :rx],
    :lrv_net => [:hml_fx_net, :rx_net],
    :verdelhan => [:carry, :dollar]
)
const COMPLETE_MODELS = Iterators.product(keys(CURRENCYRISK_MODELS), keys(BENCHMARK_MODELS))

const BETA_LAGS = 24

function main(options_folder)
    time_start = time()

    println("Loading data...")
    fund_data = load_fund_data(options_folder, select=READ_COLUMNS_FUNDS)
    currency_factors = CSV.read(INPUT_FILESTRING_CURRENCY_FACTORS, DataFrame)
    longshort_factors = CSV.read(INPUT_FILESTRING_LONGSHORT_FACTORS, DataFrame)
    market_factor = CSV.read(INPUT_FILESTRING_MARKET, DataFrame)
    risk_free = CSV.read(INPUT_FILESTRING_RISKFREE, DataFrame)
    currency_map = CSV.read(INPUT_FILESTRING_CURRENCY_MAP, DataFrame)

    fund_data.date = Dates.lastdayofmonth.(fund_data.date)

    println("Mapping countries to currencies...")
    fund_data.cur_code = map_currency(fund_data.date, fund_data.domicile, currency_map)

    full_data = (
        innerjoin(fund_data, longshort_factors, on=:date) |>
        partialjoin -> innerjoin(partialjoin, currency_factors, on=:date) |>
        partialjoin -> innerjoin(
            partialjoin, risk_free, on=[:cur_code, :date], matchmissing=:notequal
        ) |>
        partialjoin -> innerjoin(
            partialjoin, market_factor, on=[:cur_code, :date], matchmissing=:notequal
        )
    )
    
    full_data.MKT = full_data.mkt_gross - full_data.rf
    full_data.ret = full_data.ret_gross_m - full_data.rf

    println("Running regressions...")
    results_lock = ReentrantLock()

    @threads for (currency_risk_model, benchmark_model) in COMPLETE_MODELS
        benchmark_factors = BENCHMARK_MODELS[benchmark_model]
        currency_risk_factors = CURRENCYRISK_MODELS[currency_risk_model]
        complete_factors = vcat(benchmark_factors, currency_risk_factors)
        model_name = Symbol("$(benchmark_model)_$(currency_risk_model)_betas")
        
        model_results = compute_timevarying_betas(
            full_data; id_col=:fundid, date_col=:date, y=:ret, X=complete_factors
        )

        regressors = vcat(:const, complete_factors)
        add_factor_names(r) = ismissing(r) ? missing : (factors=regressors, r...)
        model_results = map(add_factor_names, model_results)

        lock(results_lock) do 
            full_data[!, model_name] = model_results
        end
    end

    if !isdir(OUTPUT_FILESTRING_BASE)
        mkpath(OUTPUT_FILESTRING_BASE)
    end

    output_filestring = joinpath(OUTPUT_FILESTRING_BASE, options_folder, "betas.csv")
    CSV.write(output_filestring, betas)

    time_duration = round(time() - time_start, digits=2)
    println("Finished fund regressions in $time_duration seconds")
end

function load_fund_data(options_folder; select)
    output_data = DataFrame[]
    folderstring = joinpath(INPUT_FILESTRING_BASE_FUNDS, options_folder)

    for file in readdir(folderstring)
        filestring = joinpath(folderstring, file)
        file_data = CSV.read(filestring, DataFrame, select=select)
        push!(output_data, file_data)
    end

    return vcat(output_data...)
end

function map_currency(date_series, country_series, currency_map)
    currency_series = Vector{Union{Missing, String}}(fill(missing, length(date_series)))

    @threads for i in 1:length(date_series)
        date = date_series[i]
        country = country_series[i]

        country_match = currency_map[!, :country_code] .== country

        date_match = not_outside.(Ref(date), currency_map.start_date, currency_map.end_date)

        matching_rows = currency_map[country_match .& date_match, :]

        nrow(matching_rows) > 1 && println("Warning: multiple match for $country on $date")

        if nrow(matching_rows) > 0
            currency_series[i] = matching_rows[1, :currency_code]
        end
    end

    return currency_series
end

function not_outside(date, start_date, end_date)
    !ismissing(start_date) && date < start_date && return false
    !ismissing(end_date) && date > end_date && return false
    return true
end
>>>>>>> parent of 5c65851 (pre-final-run with test code included)

function compute_timevarying_betas(regression_table; id_col, date_col, y, X)
    regression_results = group_transform(
        regression_table, id_col,
        [date_col, y, X...], timevarying_regressions, :results
    )

    return regression_results.results
end

function timevarying_regressions(date, y, X_cols...)
    datasize = length(y)
    X_no_constant = reduce(hcat, X_cols)
    X = hcat(ones(datasize), X_no_constant)
    
    data_start_date = first(date)
    first_beta_date = offset_monthend(data_start_date, DEFAULT_BETA_LAGS)
    first_beta_index = findfirst(>=(first_beta_date), date)

    result = Vector{Union{Missing, NamedTuple}}(fill(missing, datasize))
    isnothing(first_beta_index) && return result
    for i in first_beta_index:datasize
        sub_start_date = offset_monthend(date[i], -DEFAULT_BETA_LAGS)
        sub_start_index = findfirst(>=(sub_start_date), date)
        sub_y = y[sub_start_index:i]
        length(sub_y) <= DEFAULT_MIN_REGRESSION_OBS && continue
        sub_X = X[sub_start_index:i, :]

        nonmissing_y = findall(!ismissing, sub_y)
        complete_sub_y = Vector{Float64}(sub_y[nonmissing_y])
        complete_sub_X = sub_X[nonmissing_y, :]

        regfit = lm(complete_sub_X, complete_sub_y)

        v_coef = coef(regfit)
        v_se = stderror(regfit)
        v_df = dof_residual(regfit)

        result[i] = (coef=v_coef, se=v_se, df=v_df)
    end
    return result
end

function main(options_folder)
    time_start = time()

    full_data = initialise_main_data(options_folder)
    
    println("Running regressions...")
    full_results = copy(full_data[:, [:fundid, :date]])
    results_lock = ReentrantLock()

    @threads for (currency_risk_model, benchmark_model) in COMPLETE_MODELS
        process_start = time()
        benchmark_factors = BENCHMARK_MODELS[benchmark_model]
        currency_risk_factors = CURRENCYRISK_MODELS[currency_risk_model]
        complete_factors = vcat(benchmark_factors, currency_risk_factors)
        model_name = Symbol("$(benchmark_model)_$(currency_risk_model)_betas")
        
        model_results = compute_timevarying_betas(
            full_data; id_col=:fundid, date_col=:date, y=:ret, X=complete_factors
        )

        add_regressornames(r) = ismissing(r) ? missing : (regressors=complete_factors, r...)
        model_results = map(add_regressornames, model_results)

        lock(results_lock) do
            full_results[!, model_name] = model_results
        end

        process_elapsed = round(time() - process_start, digits=2)
        println(
            "Process finished regressing on $(benchmark_model) with " *
            "$(currency_risk_model) in $process_elapsed seconds ($(process_elapsed/60) " *
            "minutes))"
        )
    end

    dropmissing!(full_results)

    output_folderstring = joinpath(OUTPUT_FILESTRING_BASE, options_folder)
    if !isdir(output_folderstring)
        mkdir(output_folderstring)
    end

    output_filestring = joinpath(output_folderstring, "betas.csv")
    CSV.write(output_filestring, full_results)

    time_duration = round(time() - time_start, digits=2)
    println(
        "Finished fund regressions in $time_duration seconds " *
        "($(time_duration/60) minutes)"
    )
end
<<<<<<< HEAD

if abspath(PROGRAM_FILE) == @__FILE__
    options_folder = option_foldername(currency_type="local", strict_eq=true)
    main(options_folder)
end
=======
#END AUTOTEST SECTION

# for i in unique(full_data.fundid)
#     df = full_data[full_data.fundid .== i, :]
#     count = length(df.ret)
#     fullcount = length(collect(skipmissing(df.ret)))

#     if count == 49 && fullcount == 48
#         println(i)
#     end
# end

# findfirst(==("FSUSA0BHKE"), full_data.fundid)
# println(size(full_data))

# possible_matches = [
#     "FS00008RNR", "FS00008RNS", "FS0000C4UW", "FSUSA0ALMH", "FS0000ADD7", "FS0000CDZ2",
#     "FS0000CU2H", "FS0000CXN3", "FS0000CXXS", "FS0000CY4I", "FS0000D0DK", "FS00009XS4",
#     "FS0000AHCA", "FS0000CY2I", "FS0000DJ8K", "FSGBR04NIQ", "FSGBR05DB9", "FSUSA08G1P",
#     "FS0000AB4Q", "FS0000C5NL", "FS0000DRW5", "FS0000AHI1", "FS0000CTV3", "FSUSA0AURR",
#     "FS0000BPKX", "FS0000C1YB", "FS0000CPBV", "FS0000CT6T", "FS0000CYR9", "FSGBR04SI7",
#     "FSHKG08B1X"
# ]

# for i in 1:size(full_data)[1]
#     test = full_data[i:i+49, :MKT]
#     println(test)
#     println(testx[1])
#     error("STOP")
#     if full_data[i:i+49, :MKT] == testx[1]
#         println(i)
#     end
# end

# function testfunc()
#     for i in unique(full_data.fundid)
#         subdf = full_data[full_data.fundid .== i, :]
#         for j in testy
#             if j ∉ skipmissing(subdf.ret)
#                 return
#             end
#         end
#         if found
#             println(i)
#         end
#     end
# end
# testfunc()

# sample1 = full_data[full_data.ret == first(testy)]

# df2 = full_data[full_data.fundid .== "FS00008RNR", :]
# j1 = first(testy)

# j1 ∉ skipmissing(df2.ret)

# candidates = []
# for i in testy
#     subdf = full_data[isequal.(full_data.ret,  i), :]
#     if size(subdf)[1] == 0
#         continue
#     end
#     candidates_i = unique(subdf.fundid)
#     push!(candidates, candidates_i)
# end

# candidates = intersect(candidates...)
>>>>>>> parent of 5c65851 (pre-final-run with test code included)
