module RegressFundFlows
### TODO: Finish simplifying regression script ###

using Revise
using DataFrames
using Arrow
using Dates
using GLM
using Distributions

include("../../shared/CommonConstants.jl")
include("../../shared/CommonFunctions.jl")

using .CommonFunctions
using .CommonConstants

export regress_fund_flows
export _flow_regression_table

function regress_fund_flows(model_name; filter_by=nothing) 
    # model_name = "ff_usa_ffc6"; filter_by = x->x.foreign
    task_start = time()

    flow_data = initialise_flow_data(model_name)
    isnothing(filter_by) || filter!(filter_by, flow_data)

    flow_model = _flow_regression(flow_data; intercept=false)

    printtime("regressing flow betas on $model_name", task_start)
    return flow_output
end

function _flow_regression(flow_data; intercept=true)
    # intercept = false

    ret_vars = propertynames(flow_data[!, r"ret_"])
    
    # Initialised flow data doesn't contain any transformed columns, so define those
    # in the formula. GLM can't handle missings introduced via formulae, so use of
    # lag within formula is not supported and lags must be produced beforehand. The
    # flow_data frame is not reused so it is fine to modify.
    fundlag!(flow_data, :flow, 19)
    fundlag

        # X_formula = (
        #     sum(term.(ret_vars))
        #     + FunctionTerm(l, term(:flow),)
    !intercept && (X_formula = term(0) + X_formula)
    reg_formula = term(:flow) ~ term(0) + sum(term.(X_names))

    regfit = lm(reg_formula, flow_data)

    return_col_indices = findall(x->in(x,return_component_cols), Symbol.(coefnames(regfit)))

    factor_names = [
        match(r"(?<=ret_).+(?=_m1)", string(name)).match for name in return_component_cols
    ]
    
    flow_betas = DataFrame(
        factor = factor_names,
        coef = coef(regfit)[return_col_indices],
        se = stderror(regfit)[return_col_indices]
    )

    df = nrow(flow_data) - length(X_names) - 1
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