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
    n_trials = 10#0_000
    
    output_d = _create_bootstrapped_table(n_trials; filter_by=x->!x.foreign)
    output_f = _create_bootstrapped_table(n_trials; filter_by=x->x.foreign)

    output_filepath_d = makepath(DIRS.output, "domestic_coef_table.arrow")
    output_filepath_f = makepath(DIRS.output, "foreign_coef_table.arrow")

    Arrow.write(output_filepath_d, output_d)
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

    n_coefficients = nrow(coefficient_table)*(ncol(coefficient_table) - 1)
    bootstrapped_outputs = Matrix{Float64}(undef, n_coefficients, n_trials)
    for i in 1:size(bootstrapped_outputs, 2)
        # i=1
        col_size = size(bootstrapped_outputs, 1)
        boot_regression_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=filter_by, bootstrapped=true).summary
        boot_regression_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=filter_by, bootstrapped=true).summary
        _fill_coefficient_col!(
            view(bootstrapped_outputs, :, i),
            boot_regression_usa,
            boot_regression_dev
        )
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

function _fill_coefficient_col!(coefficient_col, regression_usa, regression_dev)
    # regression_usa = boot_regression_usa; regression_dev = boot_regression_dev
    usa_propα = regression_usa.coef / regression_usa.coef[1]
    dev_propα = regression_dev.coef / regression_dev.coef[1]
    dev_m_usa = [
        regression_dev.coef[1] - regression_usa.coef[1];
        dev_propα[2:end] - usa_propα[2:end]
    ]
    
    coefficient_col .= [
        regression_usa.coef;
        regression_dev.coef;
        usa_propα;
        dev_propα;
        dev_m_usa
    ]

    return coefficient_col
end

function _fill_coefficient_table!(coefficient_table, regression_usa, regression_dev)
    # coefficient_table = i; regression_usa = boot_regression_usa; regression_dev = boot_regression_dev
    coefficient_table.usa_coef = regression_usa.coef
    coefficient_table.dev_coef = regression_dev.coef

    coefficient_table.usa_propα = coefficient_table.usa_coef / coefficient_table.usa_coef[1]
    coefficient_table.dev_propα = coefficient_table.dev_coef / coefficient_table.dev_coef[1]

    coefficient_table.dev_m_usa[1] = (
        coefficient_table.dev_coef[1] - coefficient_table.usa_coef[1]
    )
    coefficient_table.dev_m_usa[2:end] = (
        coefficient_table.dev_propα[2:end] - coefficient_table.usa_propα[2:end]
    )

    return coefficient_table
end

function _fill_bootstrapped_se!(bootstrapped_se, bootstrapped_outputs)
    for i in 1:nrow(bootstrapped_se)
        for j in 1:ncol(bootstrapped_se)
            j == 1 && continue
            bootstrapped_se[i, j] = std(bootstrapped_outputs[i+j, :])
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

    output.usa_propα[1] = ""
    output.usa_propα[2:end] = _format_output_column(
        true_coefficient_table.usa_propα[2:end], bootstrapped_se.usa_propα[2:end]
    )
    output.dev_propα[1] = ""
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