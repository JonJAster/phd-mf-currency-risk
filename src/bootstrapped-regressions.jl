using Revise
using DataFrames
using Arrow
using StatsBase
using LinearAlgebra
using Distributions
using Base.Threads

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function bootstrapped_regressions()
    n_trials = 10_000
    
    output_d = _create_bootstrapped_table(n_trials; filter_by=x->!x.foreign)
    output_f = _create_bootstrapped_table(n_trials; filter_by=x->x.foreign)

    output_filepath_d = makepath(DIRS.output, "domestic_coef_table.arrow")
    output_filepath_f = makepath(DIRS.output, "foreign_coef_table.arrow")

    Arrow.write(output_filepath_d, output_d)
    Arrow.write(output_filepath_f, output_f)
end

function _create_bootstrapped_table(n_trials; filter_by=nothing)
    # n_trials = 50; filter_by=x->x.foreign

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

    task_start = time()
    for i in 1:n_trials
        # i = 1
        col_size = size(bootstrapped_outputs, 1)
        boot_regression_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=filter_by, bootstrapped=true).summary
        boot_regression_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=filter_by, bootstrapped=true).summary
        _fill_coefficient_col!(
            view(bootstrapped_outputs, :, i),
            boot_regression_usa,
            boot_regression_dev
        )
    end
    printtime("$n_trials bootstrapped regressions", task_start)

    bootstrapped_se = copy(coefficient_table)
    _fill_bootstrapped_se!(
        bootstrapped_se[!, Not([:factor, :dev_m_usa])],
        bootstrapped_outputs
    )

    dev_m_usa_idx = nrow(coefficient_table)*(ncol(coefficient_table) - 2) + 1
    dev_m_usa_prop_idx = dev_m_usa_idx + 1
    
    dev_m_usa_prop_vcov = cov(bootstrapped_outputs[dev_m_usa_prop_idx:end,:], dims=2)
    bootstrapped_se.dev_m_usa[1, :] .= std(bootstrapped_outputs[dev_m_usa_idx, :])
    bootstrapped_se.dev_m_usa[2:end, :] .= sqrt.(diag(dev_m_usa_vcov))

    true_coefficient_table = _fill_coefficient_table!(
        coefficient_table, true_regression_usa, true_regression_dev
    )
    rename!(true_regression_usa, :se => :usa_se)
    rename!(true_regression_dev, :se => :dev_se)
    true_se = hcat(true_regression_usa[!, [:usa_se]], true_regression_dev[!, [:dev_se]])

    dev_m_usa_prop_coefs = true_coefficient_table[2:end, :dev_m_usa]
    dev_m_usa_prop_sum = sum(dev_m_usa_prop_coefs)
    dev_m_usa_prop_se = sqrt(sum(dev_m_usa_prop_vcov))

    output = DataFrame(
        :factor => [
            :alpha,
            :se,
            :wret_mkt,
            :se,
            :wret_smb,
            :se,
            :wret_hml,
            :se,
            :wret_rmw,
            :se,
            :wret_cma,
            :se,
            :wret_wml,
            :se,
            :sum,
            :se
        ],
        :usa_coef => Vector{String}(undef, 16),
        :dev_coef => Vector{String}(undef, 16),
        :usa_propα => Vector{String}(undef, 16),
        :dev_propα => Vector{String}(undef, 16),
        :dev_m_usa => Vector{String}(undef, 16),
        :dev_m_usa_prop => Vector{String}(undef, 16) 
    )

    _fill_output_table!(
        output,
        true_coefficient_table,
        bootstrapped_se,
        true_se,
        dev_m_usa_prop_sum,
        dev_m_usa_prop_se
    )

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

function _fill_bootstrapped_se!(bootstrapped_se_slice, bootstrapped_outputs)
    for i in 1:ncol(bootstrapped_se_slice)
        for j in 1:nrow(bootstrapped_se_slice)
            idx = (i - 1)*nrow(bootstrapped_se_slice) + j
            bootstrapped_se_slice[j, i] = std(bootstrapped_outputs[idx, :])
        end
    end
end

function _fill_output_table!(
        output,
        true_coefficient_table,
        bootstrapped_se,
        true_se,
        dev_m_usa_prop_sum,
        dev_m_usa_prop_se
    )
    output.usa_coef[1:end-2] = _format_output_column(
        true_coefficient_table.usa_coef, true_se.usa_se
    )
    output.usa_coef[end-1:end] = ""

    output.dev_coef[1:end-2] = _format_output_column(
        true_coefficient_table.dev_coef, true_se.dev_se
    )
    output.dev_coef[end-1:end] = ""

    output.usa_propα[1] = ""
    output.usa_propα[2:end-2] = _format_output_column(
        true_coefficient_table.usa_propα[2:end], bootstrapped_se.usa_propα[2:end];
        as_percent=true
    )
    output.usa_propα[end-1:end] = ""

    output.dev_propα[1] = ""
    output.dev_propα[2:end-2] = _format_output_column(
        true_coefficient_table.dev_propα[2:end], bootstrapped_se.dev_propα[2:end];
        as_percent=true
    )
    output.dev_propα[end-1:end] = ""

    output.dev_m_usa[1] = _format_output_column(
        [true_coefficient_table.dev_m_usa[1]], [bootstrapped_se.dev_m_usa[1]]
    )
    output.dev_m_usa[2:end] .= ""

    output.dev_m_usa_prop[1] = ""
    output.dev_m_usa_prop[2:end-2] = _format_output_column(
        true_coefficient_table.dev_m_usa[2:end], bootstrapped_se.dev_m_usa[2:end];
        as_percent=true
    )
    output.dev_m_usa_prop[end-1:end] = _format_output_column(
        [dev_m_usa_prop_sum], [dev_m_usa_prop_se]; as_percent=true
    )

    return output
end

function _format_output_column(coef_col, se_col; as_percent=false)
    # coef_col = true_coefficient_table.usa_coef; se_col = true_se.usa_se; as_percent = false
    if as_percent
        coef_col .*= 100
        se_col .*= 100
        digits = 2
    else
        digits = 3
    end

    output = zip(
        ["$(round.(coef_col[i], digits=digits))" for i in eachindex(coef_col)],
        ["$(round.(se_col[i], digits=digits))" for i in eachindex(se_col)]
    ) |> Iterators.flatten |> collect

    return output
end

if isnothing(match(r"terminalserver.jl$", abspath(PROGRAM_FILE)))
    task_start = time()
    bootstrapped_regressions()
    printtime("bootstrapping regressions", task_start)
end