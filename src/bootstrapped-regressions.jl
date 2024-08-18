using Revise
using DataFrames
using Arrow
using StatsBase

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function bootstrapped_regressions()
    n_trials = 100#_000
    
    output_d = _create_bootstrapped_table(n_trials; filter_by=x->!x.foreign)
end

function _create_bootstrapped_table(n_trials; filter_by=nothing)
    # n_trials = 10; filter_by=x->!x.foreign
    task_start = time()

    # TODO - the structure of this data is designed to be readable for me, but it is
    # almost certainly slower than alternatives, like just having a single list of floats
    # and an index array.
    coefficient_table = DataFrame(
        :factor =>
            [:alpha, :wret_mkt, :wret_smb, :wret_hml, :wret_rmw, :wret_cma, :wret_wml],
        :usa_coef => Vector{Float64}(undef, 7),
        :dev_coef => Vector{Float64}(undef, 7),
        :usa_propα => Vector{Float64}(undef, 7),
        :dev_propα => Vector{Float64}(undef, 7),
        :dev_m_usa => Vector{Float64}(undef, 7)
    )

    true_regression_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=filter_by, bootstrapped=false).summary
    true_regression_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=filter_by, bootstrapped=false).summary

    bootstrapped_outputs = [copy(coefficient_table) for i in 1:n_trials]
    for (i, table_i) in enumerate(bootstrapped_outputs)
        # (i, table_i) = (1, first(bootstrapped_outputs))
        boot_regression_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=filter_by, bootstrapped=true).summary
        boot_regression_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=filter_by, bootstrapped=true).summary
        _fill_coefficient_table!(table_i, boot_regression_usa, boot_regression_dev)
        i % 5 == 0 && printtime("$(i)th bootstrapped regression", task_start)
    end

    bootstrapped_se = copy(coefficient_table)
    _fill_bootstrapped_se!(bootstrapped_se, bootstrapped_outputs)

    true_coefficient_table = _fill_coefficient_table!(
        coefficient_table, true_regression_usa, true_regression_dev
    )
    rename!(true_regression_usa, :se => :usa_se)
    rename!(true_regression_dev, :se => :dev_se)
    true_se = hcat(true_regression_usa[!, [:usa_se]], true_regression_dev[!, [:dev_se]])

    output = DataFrame(
        :factor =>
            [:alpha, :wret_mkt, :wret_smb, :wret_hml, :wret_rmw, :wret_cma, :wret_wml],
        :usa_coef => Vector{String}(undef, 7),
        :dev_coef => Vector{String}(undef, 7),
        :usa_propα => Vector{String}(undef, 7),
        :dev_propα => Vector{String}(undef, 7),
        :dev_m_usa => Vector{String}(undef, 7)
    )

    _fill_output_table!(output, true_coefficient_table, bootstrapped_se, true_se)

    return output
end

function _fill_coefficient_table!(coefficient_table, regression_usa, regression_dev)
    # coefficient_table = i; regression_usa = boot_regression_usa; regression_dev = boot_regression_dev
    coefficient_table.usa_coef = regression_usa.coef
    coefficient_table.dev_coef = regression_dev.coef

    coefficient_table.usa_propα = coefficient_table.usa_coef / coefficient_table.usa_coef[1]
    coefficient_table.dev_propα = coefficient_table.dev_coef / coefficient_table.dev_coef[1]

    coefficient_table.dev_m_usa[1] = (
        coefficient_table.usa_coef[1] - coefficient_table.dev_coef[1]
    )
    coefficient_table.dev_m_usa[2:end] = (
        coefficient_table.usa_propα[2:end] - coefficient_table.dev_propα[2:end]
    )

    return coefficient_table
end

function _fill_bootstrapped_se!(bootstrapped_se, bootstrapped_outputs)
    for i in 1:nrow(bootstrapped_se)
        for j in 1:ncol(bootstrapped_se)
            j == 1 && continue
            bootstrapped_se[i, j] = std([x[i, j] for x in bootstrapped_outputs])
        end
    end
end

function _fill_output_table!(output, true_coefficient_table, bootstrapped_se, true_se)
    output.usa_coef = _format_output_column(
        true_coefficient_table.usa_coef, true_se.usa_se
    )
    output.dev_coef = _format_output_column(
        true_coefficient_table.dev_coef, true_se.dev_se
    )

    output.usa_propα[2:end] = _format_output_column(
        true_coefficient_table.usa_propα[2:end], bootstrapped_se.usa_propα[2:end]
    )
    output.dev_propα[2:end] = _format_output_column(
        true_coefficient_table.dev_propα[2:end], bootstrapped_se.dev_propα[2:end]
    )

    output.dev_m_usa = _format_output_column(
        true_coefficient_table.dev_m_usa, bootstrapped_se.dev_m_usa
    )

    return output
end

function _format_output_column(coef, se)
    return [
        "$(round(coef[i], digits=3)) ± $(round(se[i], digits=3))"
        for i in 1:length(coef)
    ]
end