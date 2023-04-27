using DataFrames
using CSV
using GLM
using Dates
using Base.Threads
using JLD2

include("CommonFunctions.jl")
include("CommonConstants.jl")
include("DataReader.jl")
using .CommonFunctions
using .CommonConstants
using .DataReader

const OUTPUT_FILESTRING_BASE = "./data/results/"
const DATA_FORMATTING_DESCRIPTION = [
    ["Regression betas (excluding constant)"],
    ["Standard errors of currency factors only"],
    "Degrees of freedom"
]
const DATA_FORMATTING_FUNCTIONS = (
    format_coefs = regfit -> coef(regfit) |> non_constant_coefs .|> n -> round(n, digits=5),
    format_se = regfit -> stderror(regfit) |> currency_coefs .|> n -> round(n, digits=5),
    format_df = regfit -> dof_residual(regfit)
)
const DATA_FORMATTING_TYPE = Tuple{Vector{Float64}, Vector{Float64}, Float64}

non_constant_coefs(coef_vector) = coef_vector[2:end]
currency_coefs(coef_vector) = coef_vector[end-2:end]

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

    result = Vector{Union{Missing, DATA_FORMATTING_TYPE}}(fill(missing, datasize))
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

        v_coef = DATA_FORMATTING_FUNCTIONS.format_coefs(regfit)
        v_se = DATA_FORMATTING_FUNCTIONS.format_se(regfit)
        v_df = DATA_FORMATTING_FUNCTIONS.format_df(regfit)

        result[i] = (v_coef, v_se, v_df)
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

    println("Saving results...")
    output_folderstring = joinpath(OUTPUT_FILESTRING_BASE, options_folder)
    if !isdir(output_folderstring)
        mkdir(output_folderstring)
    end

    output_filestring = joinpath(output_folderstring, "betas.jld2")
    save(
        output_filestring, Dict(
            "data"=>full_results,
            "data_description"=>DATA_FORMATTING_DESCRIPTION
        )
    )

    time_duration = round(time() - time_start, digits=2)
    println(
        "Finished fund regressions in $time_duration seconds " *
        "($(time_duration/60) minutes)"
    )
end

if abspath(PROGRAM_FILE) == @__FILE__
    options_folder = option_foldername(currency_type="local", strict_eq=true)
    main(options_folder)
end