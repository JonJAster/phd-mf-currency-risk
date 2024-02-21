using Revise
using DataFrames
using Arrow
using GLM
using Dates
using Base.Threads

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function regress_fund_returns(model_name)
    task_start = time()

    model = MODELS[model_name]
    regression_data = initialise_base_data(model)

    model_factors = model[2]
    model_result = _compute_timevarying_betas(
        regression_data; id_col=:fundid, date_col=:date, y=:ex_ret, X=model_factors
    )

    printtime("regressing fund returns for model $model_name", task_start)
    return model_result
end

function _compute_timevarying_betas(regression_data; id_col, date_col, y, X)
    output_cols = [:coef, :se, :df]
    regression_results = combine(
        groupby(regression_data, id_col),
        [date_col, y, X...] => _timevarying_regressions => [date_col, output_cols...]
    )
    
    regressor_list = vcat(:const, X...)
    n_repeats, fit_check = divrem(size(regression_results, 1), length(regressor_list))

    perfect_multiple = 0
    @assert fit_check == perfect_multiple

    regression_results.factor = repeat(regressor_list, n_repeats)

    return regression_results[:, [id_col, date_col, :factor, output_cols...]]
end

function _timevarying_regressions(date, y, X_cols...)
    n_obs = length(y)
    X_no_constant = reduce(hcat, X_cols)
    X = hcat(ones(n_obs), X_no_constant)
    n_factors = size(X, 2)

    data_start_date = first(date)
    first_beta_date = data_start_date + Month(BETA_LAGS)
    first_beta_index = findfirst(>=(first_beta_date), date)

    coef_out = Matrix{Union{Missing, Float64}}(missing, n_factors, n_obs)
    se_out = Matrix{Union{Missing, Float64}}(missing, n_factors, n_obs)
    df_out = Matrix{Union{Missing, Float64}}(missing, n_factors, n_obs)

    if !isnothing(first_beta_index)
		@threads for i in first_beta_index:n_obs
			sub_start_date = date[i] - Month(BETA_LAGS)
			sub_start_index = findfirst(>=(sub_start_date), date)
			sub_y = y[sub_start_index:i-1]
			sub_X = X[sub_start_index:i-1, :]

			nonmissing_y = findall(!ismissing, sub_y)
			complete_sub_y = Vector{Float64}(sub_y[nonmissing_y])
            length(complete_sub_y) <= MIN_REGRESSION_OBS && continue
			complete_sub_X = sub_X[nonmissing_y, :]

			regfit = lm(complete_sub_X, complete_sub_y)

			coef_out[:, i] = coef(regfit)
			se_out[:, i] = stderror(regfit)
			df_out[:, i] = [dof_residual(regfit) for _ in 1:n_factors]
		end
	end
    
    return _combine_reg_results(date, coef_out, se_out, df_out)
end

function main()
    for model_name in keys(MODELS)
        # This loop is inefficient in that it reads the same data for each model, but is the
        # lowest effort way to enable calling the function for a single model from
        # elsewhere.
        output_data = regress_fund_returns(model_name)
        output_filename = makepath(DIRS.combo.return_betas, "$model_name.arrow")
        
        Arrow.write(output_filename, output_data)
    end
    return
end

_combine_reg_results(date, rr...) = zip(repeat(date, inner=size(first(rr),1)), vec.(rr)...)

if abspath(PROGRAM_FILE) == @__FILE__
    task_start = time()
    main()
    printtime("regressing all fund returns", task_start; minutes=true)
end