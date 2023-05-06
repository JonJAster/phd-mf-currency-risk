using DataFrames
using Arrow
using GLM
using Dates
using Base.Threads

include("CommonFunctions.jl")
include("CommonConstants.jl")
include("DataReader.jl")
using .CommonFunctions
using .CommonConstants
using .DataReader

const OUTPUT_FILESTRING_BASE = "./data/results/"
const REGRESSION_OUTPUT_COLS = [:coef, :se, :df]

const PERFECT_MULTIPLE = 0

@inline currency_coefs(coef_vector) = coef_vector[end-2:end]
@inline combine_reg_results(date, rr...) = zip(repeat(date, inner=size(first(rr),1)), vec.(rr)...)

function compute_timevarying_betas(regression_table; id_col, date_col, y, X)
    regression_results = group_combine(
        regression_table, id_col,
        [date_col, y, X...], timevarying_regressions, [date_col, REGRESSION_OUTPUT_COLS...],
        cast=false
    )

    regressor_list = vcat(:const, X...)
    n_repeats, fit_check = divrem(size(regression_results, 1), length(regressor_list))
    @assert fit_check == PERFECT_MULTIPLE
    regression_results.factor = repeat(regressor_list, n_repeats)

    return regression_results[:, [id_col, date_col, :factor, REGRESSION_OUTPUT_COLS...]]
end

function timevarying_regressions(date, y, X_cols...)
    n_obs = length(y)
    X_no_constant = reduce(hcat, X_cols)
    X = hcat(ones(n_obs), X_no_constant)
    n_factors = size(X, 2)

    data_start_date = first(date)
    first_beta_date = offset_monthend(data_start_date, DEFAULT_BETA_LAGS)
    first_beta_index = findfirst(>=(first_beta_date), date)

    coef_out = Matrix{Union{Missing, Float64}}(missing, n_factors, n_obs)
    se_out = Matrix{Union{Missing, Float64}}(missing, n_factors, n_obs)
    df_out = Matrix{Union{Missing, Float64}}(missing, n_factors, n_obs)

    if !isnothing(first_beta_index)
		@threads for i in first_beta_index:n_obs
			sub_start_date = offset_monthend(date[i], -DEFAULT_BETA_LAGS)
			sub_start_index = findfirst(>=(sub_start_date), date)
			sub_y = y[sub_start_index:i]
			sub_X = X[sub_start_index:i, :]

			nonmissing_y = findall(!ismissing, sub_y)
			complete_sub_y = Vector{Float64}(sub_y[nonmissing_y])
            length(complete_sub_y) <= DEFAULT_MIN_REGRESSION_OBS && continue
			complete_sub_X = sub_X[nonmissing_y, :]

			regfit = lm(complete_sub_X, complete_sub_y)

			coef_out[:, i] = coef(regfit)
			se_out[:, i] = stderror(regfit)
			df_out[:, i] = [dof_residual(regfit) for _ in 1:n_factors]
		end
	end
    
    return combine_reg_results(date, coef_out, se_out, df_out)
end

function main(options_folder)
    time_start = time()

    full_data = initialise_main_data(options_folder)
    
    println("Running regressions...")
    model_results = Dict(model=>full_data[:, [:fundid, :date]] for model in COMPLETE_MODELS)

    @threads for model in COMPLETE_MODELS
        process_start = time()
        currency_risk_model_name, benchmark_model_name = model
        benchmark_factors = BENCHMARK_MODELS[benchmark_model_name]
        currency_risk_factors = CURRENCYRISK_MODELS[currency_risk_model_name]
        complete_factors = vcat(benchmark_factors, currency_risk_factors)
        
        model_result = compute_timevarying_betas(
            full_data; id_col=:fundid, date_col=:date, y=:ret, X=complete_factors
        )

        dropmissing!(model_result)

        model_results[model] = model_result
        
        process_elapsed_s = round(time() - process_start, digits=2)
        process_elapsed_m = round(process_elapsed_s/60, digits=2)
        println(
            "Process finished regressing on $(benchmark_model_name) with " *
            "$(currency_risk_model_name) in $process_elapsed_s seconds " *
            "($process_elapsed_m minutes)"
        )
    end

    println("Saving results...")
    output_folderstring = joinpath(OUTPUT_FILESTRING_BASE, options_folder, "betas")

    if !isdir(output_folderstring)
        mkdir(output_folderstring)
    end

    for model in COMPLETE_MODELS
        model_name = name_model(model)
		filestring = joinpath(output_folderstring, "$model_name.arrow")

        Arrow.write(filestring, model_results[(currency_risk, benchmark)])
    end

    time_duration_s = round(time() - time_start, digits=2)
    time_duration_m = round(time_duration_s/60, digits=2)
    println(
        "Finished fund regressions in $time_duration_s seconds " *
        "($time_duration_m minutes)"
    )
end

if abspath(PROGRAM_FILE) == @__FILE__
    options_folder = option_foldername(currency_type="local", strict_eq=true)
    main(options_folder)
end

dfb = CSV.read("data/prepared/mutual-funds/local-monthly-gross-returns/mf_local-monthly-gross-returns_can-chn-jpn.csv", DataFrame)