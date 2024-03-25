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

function regress_fund_flows(model_name; filter_by=nothing)
    task_start = time()

    flow_data = _initialise_flow_data(model_name)

    if !isnothing(filter_by)
        info_filename = joinpath(DIRS.mf.init, "mf-info.arrow")
        info = loadarrow(info_filename)

        filtered_ids = info[filter_by(info),[:fundid]]
        flow_data = innerjoin(flow_data, filtered_ids, on=:fundid)
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

    flow_betas = _flow_regression(regression_data, return_component_cols)

    printtime("regressing flow betas on $model_name", task_start)
    return flow_betas
end

function _initialise_flow_data(model_name)
    filename_mf = joinpath(DIRS.mf.refined, "mf-data.arrow")
    filename_info = joinpath(DIRS.mf.refined, "mf-info.arrow")
    filename_decomposition = joinpath(DIRS.combo.weighted, "$model_name.arrow")

    fund_base_data = loadarrow(filename_mf)
    fund_info = loadarrow(filename_info)
    decomposed_returns = loadarrow(filename_decomposition)

    fund_base_data.std_return_12m = rolling_std(fund_base_data, :ex_ret, 12; lagged=true)

    select!(
        fund_base_data,
        [:fundid, :date, :flow, :net_assets_m1, :costs, :std_return_12m]
    )
    select!(fund_info, [:fundid, :true_no_load, :inception_date])

    fund_rets_data = innerjoin(fund_base_data, decomposed_returns, on=[:fundid, :date])

    fund_full_data = innerjoin(
        fund_rets_data, fund_info, on=:fundid, matchmissing=:notequal
    )

    fund_full_data.age = (
        12*(year.(fund_full_data.date) .- year.(fund_full_data.inception_date)) .+
        (month.(fund_full_data.date) .- month.(fund_full_data.inception_date)) .+ 1
    )

    output_data = fund_full_data[fund_full_data.age .>= AGE_FILTER, :]

    output_data.log_lag_size = log.(output_data.net_assets_m1)
    output_data.log_age = log.(output_data.age)
    
    sort!(output_data, [:fundid, :date])
    select!(output_data, Not(["inception_date", "age", "net_assets_m1"]))

    return output_data
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