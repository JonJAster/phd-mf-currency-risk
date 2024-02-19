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

function regress_fund_returns(model) # model = ("USA", ["mkt", "dollar", "carry"])
    task_start = time()

    mf_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")
    factors_filename = joinpath(DIRS.combo.factors, "factors.arrow")

    mf_data = Arrow.Table(mf_filename) |> DataFrame
    factors_data = Arrow.Table(factors_filename) |> DataFrame

    _prepare_factors!(factors_data, model)

    regression_data = innerjoin(mf_data, factors_data, on=:date)

    return_betas = _regress_returns(regression_data)

    printtime("regressing fund returns for model $model", task_start)
    return return_betas
end

function _prepare_factors!(factors_data, model) 
    model_region = model[1]
    model_factors = model[2]

    region_condition = (
        factors_data.region .== model_region .||
        factors_data.region .== "FX"
    )

    factor_condition = in.(factors_data.factor, Ref(model_factors))

    regioned_factors = factors_data[region_condition .&& factor_condition, :]
    maximum(factors_data[factors_data.region .!= "FX", :])
    wide_factors = unstack(regioned_factors, :date, :factor, :ret)

    return wide_factors
end

function main()
    for model_key_value in MODELS
        # This loop is inefficient in that it reads the same data for each model, but is the
        # lowest effort way to enable calling the function for a single model from
        # elsewhere.
        model_name = model_key_value[1]
        model = model_key_value[2]
        output_data = regress_fund_returns(model)
        output_filename = makepath(DIRS.combo.return_betas, "$model_name.arrow")

        task_start = time()
        Arrow.write(output_filename, output_data)
        printtime("writing regressed returns for model $model_name", task_start)
    end
    return
end

_currency_coefs(coef_vector) = coef_vector[end-2:end]
_combine_reg_results(date, rr...) = zip(repeat(date, inner=size(first(rr),1)), vec.(rr)...)

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end