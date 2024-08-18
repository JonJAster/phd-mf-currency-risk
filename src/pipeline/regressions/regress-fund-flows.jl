module RegressFundFlows

#using Revise
using DataFrames
using Arrow
using Dates
using GLM
using Distributions

include("../../shared/CommonConstants.jl") #includet("../../shared/CommonConstants.jl")
include("../../shared/CommonFunctions.jl") #includet("../../shared/CommonFunctions.jl") 

using .CommonFunctions
using .CommonConstants

export regress_fund_flows
export _flow_regression_table

function regress_fund_flows(
        model_name;
        filter_by=nothing, ret_type=:decomposed, bootstrapped=false
    ) 
    # model_name = "ff_usa_ffc6"; filter_by = x->x.foreign; ret_type=:ret; bootstrapped=false
    
    flow_data = initialise_flow_data(model_name, bootstrapped=bootstrapped)
    isnothing(filter_by) || filter!(filter_by, flow_data)

    flow_output = _flow_regression(flow_data; intercept=false, ret_type=ret_type)

    return flow_output
end

function _flow_regression(flow_data; intercept=true, ret_type=:decomposed)
    # intercept = false

    if ret_type == :decomposed
        select!(flow_data, Not(:ret_m1))
        ret_vars = propertynames(flow_data[!, r"ret_"])
    elseif ret_type == :ret
        select!(flow_data, Not(r"ret_.+_m1"))
        ret_vars = [:ret_m1]
    end
    
    # Initialised flow data doesn't contain any transformed columns, so define those
    # in the formula. GLM can't handle missings introduced via formulae, so use of
    # lag within formula is not supported and lags must be produced beforehand. The
    # flow_data frame is not reused so it is fine to modify.

    # TODO: Currently avoiding programatic FunctionTerm's altogether from lack of
    #       understanding. May be able to simplify in the future.
    fundlag!(flow_data, :flow, FLOW_CONTROL_LAGS; drop=false)
    fundlag!(flow_data, :costs)
    fundlag!(flow_data, :age)
    flow_data.log_size_lag1 = log.(flow_data.net_assets_m1)
    flow_data.log_age_lag1 = log.(flow_data.age_lag1)
    select!(flow_data, Not(:net_assets_m1, :age_lag1))
    flow_data.yearmonth = yearmonth.(flow_data.date)

    X_formula = (
        sum(term.(ret_vars))
        + term("flow_lag$FLOW_CONTROL_LAGS")
        + term(:std_return_12m)
        + term(:usa_correlation_12m)
        + term(:no_load)
        + term(:costs_lag1)
        + term(:log_size_lag1)
        + term(:log_age_lag1)
        + term(:yearmonth)
    )
    if intercept
        reg_formula = term(:flow) ~ X_formula
    else
        reg_formula = term(:flow) ~ term(0) + X_formula
    end

    regfit = lm(reg_formula, flow_data)

    return_col_indices = findall(x->in(x,ret_vars), Symbol.(coefnames(regfit)))

    factor_names = [
        name == :ret_m1 ? "total_ret" : match(r"(?<=ret_).*(?=_m1)", string(name)).match
        for name in ret_vars
    ]
    
    flow_betas = DataFrame(
        factor = factor_names,
        coef = coef(regfit)[return_col_indices],
        se = stderror(regfit)[return_col_indices]
    )

    df = nrow(flow_data) - length(X_formula)
    flow_betas.tstat = flow_betas.coef ./ flow_betas.se
    flow_betas.pval = 2 * cdf(TDist(df), -abs.(flow_betas.tstat))

    output = (
        summary = flow_betas,
        regfit = regfit
    )

    return output
end

function _drop_zero_cols!(data)
    zero_cols = []
    for col in names(data)
        all(data[!, col] .== 0) && push!(zero_cols, col)
    end

    select!(data, Not(zero_cols))
end

function main()
    for model_name in keys(MODELS)
        output_data = regress_fund_flows(model_name).summary
        output_filename = makepath(DIRS.combo.flow_betas, "$model_name.arrow")

        Arrow.write(output_filename, output_data)
    end
    return
end

if isnothing(match(r"terminalserver.jl$", PROGRAM_FILE))
    task_start = time()
    main()
    printtime("regressing all flows", task_start; minutes=true)
end

end # module RegressFundFlows