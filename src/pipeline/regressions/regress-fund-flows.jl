module RegressFundFlows

#using Revise
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
export flow_regression_table

function regress_fund_flows(model_name; filter_by=nothing)
    task_start = time()

    regression_packet = flow_regression_table(model_name; filter_by=filter_by)

    regression_data = regression_packet.regression_data
    return_component_cols = regression_packet.return_component_cols

    flow_betas = _flow_regression(regression_data, return_component_cols)

    printtime("regressing flow betas on $model_name", task_start)
    return flow_betas
end

function flow_regression_table(model_name; filter_by=nothing)
    flow_data = initialise_flow_data(model_name)

    if !isnothing(filter_by)
        flow_data = filter_fundids(filter_by, flow_data)
    end

    cols = names(flow_data)
    find_return_col(name) = !isnothing(match(r"ret_", name))
    return_component_cols = cols[find_return_col.(cols)] .|> Symbol

    regression_data = regression_table(
        flow_data, :fundid, :date,
        :flow, :plus_lag, FLOW_CONTROL_LAGS,
        return_component_cols...,
        :costs, :lag, FLOW_CONTROL_LAGS,
        :true_no_load,
        :std_return_12m,
        :log_lag_size,
        :log_age, :lag,
        :tfe, :month
    )
    
    dropmissing!(regression_data)
    _drop_zero_cols!(regression_data)

    output = (
        regression_data = regression_data,
        return_component_cols = return_component_cols
    )

    return output
end

function _flow_regression(regression_data, return_component_cols)
    X_names = regression_data[!, Not([:fundid, :date, :flow])] |> names
    reg_formula = term(:flow) ~ sum(term.(X_names))

    regfit = lm(reg_formula, regression_data)
    return_col_indices = findall(x->in(x,return_component_cols), Symbol.(coefnames(regfit)))
    
    flow_betas = DataFrame(
        factor = return_component_cols,
        coef = coef(regfit)[return_col_indices],
        se = stderror(regfit)[return_col_indices]
    )

    df = nrow(regression_data) - length(X_names) - 1
    flow_betas.tstat = flow_betas.coef ./ flow_betas.se
    flow_betas.pval = 2 * cdf(TDist(df), -abs.(flow_betas.tstat))

    return flow_betas
end

function _drop_zero_cols!(data)
    zero_cols = []
    for col in names(data)
        all(data[!, col] .== 0) && push!(zero_cols, col)
    end
    
    if length(zero_cols) != FLOW_CONTROL_LAGS
        println(
            "Warning: FLOW_CONTROL_LAGS is $FLOW_CONTROL_LAGS, but " *
            "found $(length(zero_cols)) zero columns to drop."
        )
    end
    select!(data, Not(zero_cols))
end

function main()
    for model_name in keys(MODELS)
        output_data = regress_fund_flows(model_name)
        output_filename = makepath(DIRS.combo.flow_betas, "$model_name.arrow")

        Arrow.write(output_filename, output_data)
    end
    return
end

if abspath(PROGRAM_FILE) == @__FILE__
    task_start = time()
    main()
    printtime("regressing all flows", task_start; minutes=true)
end

end # module RegressFundFlows