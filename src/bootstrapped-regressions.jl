using Revise
using DataFrames
using Arrow
using Dates
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
    n_trials = 10_#0#0#0

    # Table 2: Main results
    
    output_d = _create_bootstrapped_main(n_trials; filter_by=x->!x.foreign)
    output_f = _create_bootstrapped_main(n_trials; filter_by=x->x.foreign)

    mprint(output_d)
    println()
    println()
    mprint(output_f)
    println()
    println()

    output_filepath_d = makepath(DIRS.output, "domestic_coef_table.arrow")
    output_filepath_f = makepath(DIRS.output, "foreign_coef_table.arrow")

    Arrow.write(output_filepath_d, output_d)
    Arrow.write(output_filepath_f, output_f)

    # Table 3: Add currency factors
    # TODO: This should obviously be a single function to avoid copied code if ever
    #       time permits.
    ### output_curr = _create_bootstrapped_curr(n_trials)
    output_curr = loadarrow(joinpath(DIRS.output, "curr_coef_table.arrow"))

    mprint(output_curr)
    println()
    println()

    output_filepath_curr = makepath(DIRS.output, "curr_coef_table.arrow")

    Arrow.write(output_filepath_curr, output_curr)

    # Table 4: Early v Late

    output_dated_full = _create_bootstrapped_dated(
        n_trials; filter_by=x->true, model="ff_usa_ffc6"
    )
    output_dated_d = _create_bootstrapped_dated(
        n_trials; filter_by=x->!x.foreign, model="ff_usa_ffc6"
    )
    output_dated_f = _create_bootstrapped_dated(
        n_trials; filter_by=x->x.foreign, model="ff_dev_ffc6"
    )

    mprint(output_dated_full)
    println()
    println()
    mprint(output_dated_d)
    println()
    println()
    mprint(output_dated_f)
    println()
    println()

    output_filepath_dated_d = makepath(DIRS.output, "dated_domestic_coef_table.arrow")
    output_filepath_dated_f = makepath(DIRS.output, "dated_foreign_coef_table.arrow")

    Arrow.write(output_filepath_dated_d, output_dated_d)
    Arrow.write(output_filepath_dated_f, output_dated_f)
end

function _create_bootstrapped_main(n_trials; filter_by=nothing)
    # n_trials = 10; filter_by=x->!x.foreign

    coefficient_table = DataFrame(
        :factor =>
            [:alpha, :wret_mkt, :wret_smb, :wret_hml, :wret_rmw, :wret_cma, :wret_wml],
        :usa_coef => Vector{Float64}(undef, 7),
        :dev_coef => Vector{Float64}(undef, 7),
        :usa_propα => Vector{Float64}(undef, 7),
        :dev_propα => Vector{Float64}(undef, 7),
        :dev_m_usa => Vector{Float64}(undef, 7),
        :dev_m_usa_prop => Vector{Float64}(undef, 7)
    )

    true_regression_usa = regress_fund_flows("ff_usa_ffc6"; filter_by=filter_by, bootstrapped=false).summary
    true_regression_dev = regress_fund_flows("ff_dev_ffc6"; filter_by=filter_by, bootstrapped=false).summary

    n_coefficients = nrow(coefficient_table)*(ncol(coefficient_table) - 1)
    bootstrapped_outputs = Matrix{Float64}(undef, n_coefficients, n_trials)

    task_start = time()
    for i in 1:n_trials
        # i = 1
        col_size = size(bootstrapped_outputs, 1)
        
        boot_regression_usa = regress_fund_flows(
            "ff_usa_ffc6"; filter_by=filter_by, bootstrapped=true
        ).summary
        boot_regression_dev = regress_fund_flows(
            "ff_dev_ffc6"; filter_by=filter_by, bootstrapped=true
        ).summary

        _fill_coefficient_col!(
            view(bootstrapped_outputs, :, i),
            boot_regression_usa,
            boot_regression_dev
        )
    end
    printtime("$n_trials bootstrapped regressions", task_start)

    bootstrapped_se = copy(coefficient_table)
    _fill_bootstrapped_se!(
        bootstrapped_se[!, Not([:factor, :dev_m_usa, :dev_m_usa_prop])],
        bootstrapped_outputs
    )

    dev_m_usa_idx = nrow(coefficient_table)*(ncol(coefficient_table) - 3) + 1
    dev_m_usa_prop_idx = nrow(coefficient_table)*(ncol(coefficient_table) - 2) + 1
    
    dev_m_usa_vcov = cov(bootstrapped_outputs[dev_m_usa_idx:dev_m_usa_prop_idx-1,:], dims=2)
    bootstrapped_se.dev_m_usa .= sqrt.(diag(dev_m_usa_vcov))

    dev_m_usa_prop_vcov = cov(bootstrapped_outputs[dev_m_usa_prop_idx:end,:], dims=2)
    dev_m_usa_prop_ff3_vcov = cov(
        bootstrapped_outputs[dev_m_usa_prop_idx+1:dev_m_usa_prop_idx+3,:], dims=2
    )
    bootstrapped_se.dev_m_usa_prop .= sqrt.(diag(dev_m_usa_prop_vcov))

    true_coefficient_table = _fill_coefficient_table!(
        coefficient_table, true_regression_usa, true_regression_dev
    )
    rename!(true_regression_usa, :se => :usa_se)
    rename!(true_regression_dev, :se => :dev_se)
    true_se = hcat(true_regression_usa[!, [:usa_se]], true_regression_dev[!, [:dev_se]])

    dev_m_usa_sum = sum(true_coefficient_table.dev_m_usa)
    dev_m_usa_se = sqrt(sum(dev_m_usa_vcov))

    dev_m_usa_prop_sum = sum(true_coefficient_table[2:end, :dev_m_usa_prop])
    dev_m_usa_prop_ff3_sum = sum(true_coefficient_table[2:4, :dev_m_usa_prop])
    dev_m_usa_prop_se = sqrt(sum(dev_m_usa_prop_vcov))
    dev_m_usa_prop_ff3_se = sqrt(sum(dev_m_usa_prop_ff3_vcov))

    sum_row = DataFrame(
        :factor => [:sum, :se],
        :dev_m_usa => [dev_m_usa_sum, dev_m_usa_se],
        :dev_m_usa_prop => [dev_m_usa_prop_sum, dev_m_usa_prop_se],
        :dev_m_usa_prop_ff3 => [dev_m_usa_prop_ff3_sum, dev_m_usa_prop_ff3_se]
    )

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
            :ff3_sum,
            :se,
            :sum,
            :se
        ],
        :usa_coef => Vector{String}(undef, 22),
        :dev_coef => Vector{String}(undef, 22),
        :usa_propα => Vector{String}(undef, 22),
        :dev_propα => Vector{String}(undef, 22),
        :dev_m_usa => Vector{String}(undef, 22),
        :dev_m_usa_prop => Vector{String}(undef, 22) 
    )

    _fill_output_table!(
        output,
        true_coefficient_table,
        bootstrapped_se,
        true_se,
        sum_row
    )

    return output
end

function _create_bootstrapped_curr(n_trials)
    # n_trials = 10

    coefficient_table = DataFrame(
        :factor => [
            :alpha,
            :wret_mkt,
            :wret_smb,
            :wret_hml,
            :wret_rmw,
            :wret_cma,
            :wret_wml,
            :wret_dollar,
            :wret_carry
        ],
        :domestic_coef => Vector{Float64}(undef, 9),
        :foreign_coef => Vector{Float64}(undef, 9),
        :domestic_propα => Vector{Float64}(undef, 9),
        :foreign_propα => Vector{Float64}(undef, 9)
    )

    true_regression_domestic = regress_fund_flows(
        "ff_usa_ffc6_ver"; filter_by=x->!x.foreign, bootstrapped=false
    ).summary
    true_regression_foreign = regress_fund_flows(
        "ff_dev_ffc6_ver"; filter_by=x->x.foreign, bootstrapped=false
    ).summary

    n_coefficients = nrow(coefficient_table)*(ncol(coefficient_table) - 1)
    bootstrapped_outputs = Matrix{Float64}(undef, n_coefficients, n_trials)

    task_start = time()
    for i in 1:n_trials
        # i = 1
        col_size = size(bootstrapped_outputs, 1)
        
        boot_regression_domestic = regress_fund_flows(
            "ff_usa_ffc6_ver"; filter_by=x->!x.foreign, bootstrapped=true
        ).summary
        boot_regression_foreign = regress_fund_flows(
            "ff_dev_ffc6_ver"; filter_by=x->x.foreign, bootstrapped=true
        ).summary

        _fill_coefficient_col_curr!(
            view(bootstrapped_outputs, :, i),
            boot_regression_domestic,
            boot_regression_foreign
        )
    end
    printtime("$n_trials bootstrapped regressions", task_start)

    bootstrapped_se = copy(coefficient_table)
    _fill_bootstrapped_se!(
        bootstrapped_se[!, Not([:factor])],
        bootstrapped_outputs
    )

    true_coefficient_table = _fill_coefficient_table_curr!(
        coefficient_table, true_regression_domestic, true_regression_foreign
    )
    rename!(true_regression_domestic, :se => :domestic_se)
    rename!(true_regression_foreign, :se => :foreign_se)
    true_se = hcat(
        true_regression_domestic[!, [:domestic_se]],
        true_regression_foreign[!, [:foreign_se]]
    )

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
            :wret_dollar,
            :se,
            :wret_carry,
            :se
        ],
        :domestic_coef => Vector{String}(undef, 18),
        :foreign_coef => Vector{String}(undef, 18),
        :domestic_propα => Vector{String}(undef, 18),
        :foreign_propα => Vector{String}(undef, 18)
    )

    _fill_output_table_curr!(
        output,
        true_coefficient_table,
        bootstrapped_se,
        true_se
    )

    return output
end

function _create_bootstrapped_dated(n_trials; filter_by, model)
    # n_trials = 10; filter_by=x->!x.foreign; model="ff_usa_ffc6"

    coefficient_table = DataFrame(
        :factor =>
            [:alpha, :wret_mkt, :wret_smb, :wret_hml, :wret_rmw, :wret_cma, :wret_wml],
        :early_coef => Vector{Float64}(undef, 7),
        :late_coef => Vector{Float64}(undef, 7),
        :early_propα => Vector{Float64}(undef, 7),
        :late_propα => Vector{Float64}(undef, 7),
        :late_m_early => Vector{Float64}(undef, 7),
        :late_m_early_prop => Vector{Float64}(undef, 7)
    )

    early_filter(x) = filter_by(x) && (x.date < Date(2010))
    late_filter(x) = filter_by(x) && (x.date >= Date(2011))
    
    true_regression_early = regress_fund_flows(
        model; filter_by=early_filter, bootstrapped=false
    ).summary
    true_regression_late = regress_fund_flows(
        model; filter_by=late_filter, bootstrapped=false
    ).summary

    n_coefficients = nrow(coefficient_table)*(ncol(coefficient_table) - 1)
    bootstrapped_outputs = Matrix{Float64}(undef, n_coefficients, n_trials)

    task_start = time()
    for i in 1:n_trials
        # i = 1
        
        boot_regression_early = regress_fund_flows(
            model; filter_by=early_filter, bootstrapped=true
        ).summary
        boot_regression_late = regress_fund_flows(
            model; filter_by=late_filter, bootstrapped=true
        ).summary

        _fill_coefficient_col_dated!(
            view(bootstrapped_outputs, :, i),
            boot_regression_early,
            boot_regression_late
        )
    end
    printtime("$n_trials bootstrapped regressions", task_start)

    bootstrapped_se = copy(coefficient_table)
    _fill_bootstrapped_se!(
        bootstrapped_se[!, Not([:factor, :late_m_early, :late_m_early_prop])],
        bootstrapped_outputs
    )

    late_m_early_idx = nrow(coefficient_table)*(ncol(coefficient_table) - 3) + 1
    late_m_early_prop_idx = nrow(coefficient_table)*(ncol(coefficient_table) - 2) + 1
    
    late_m_early_vcov = cov(
        bootstrapped_outputs[late_m_early_idx:late_m_early_prop_idx-1,:], dims=2
    )
    bootstrapped_se.late_m_early .= sqrt.(diag(late_m_early_vcov))

    late_m_early_prop_vcov = cov(
        bootstrapped_outputs[late_m_early_prop_idx:end,:], dims=2
    )
    bootstrapped_se.late_m_early_prop .= sqrt.(diag(late_m_early_prop_vcov))

    true_coefficient_table = _fill_coefficient_table_dated!(
        coefficient_table, true_regression_early, true_regression_late
    )
    rename!(true_regression_early, :se => :early_se)
    rename!(true_regression_late, :se => :late_se)
    true_se = hcat(
        true_regression_early[!, [:early_se]], true_regression_late[!, [:late_se]]
    )

    late_m_early_sum = sum(true_coefficient_table.late_m_early)
    late_m_early_se = sqrt(sum(late_m_early_vcov))

    late_m_early_prop_sum = sum(true_coefficient_table[2:end, :late_m_early_prop])
    late_m_early_prop_se = sqrt(sum(late_m_early_prop_vcov))

    sum_row = DataFrame(
        :factor => [:sum, :se],
        :late_m_early => [late_m_early_sum, late_m_early_se],
        :late_m_early_prop => [late_m_early_prop_sum, late_m_early_prop_se]
    )

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
        :early_coef => Vector{String}(undef, 16),
        :late_coef => Vector{String}(undef, 16),
        :early_propα => Vector{String}(undef, 16),
        :late_propα => Vector{String}(undef, 16),
        :late_m_early => Vector{String}(undef, 16),
        :late_m_early_prop => Vector{String}(undef, 16)
    )

    _fill_output_table_dated!(
        output,
        true_coefficient_table,
        bootstrapped_se,
        true_se,
        sum_row
    )

    return output
end

function _fill_coefficient_col!(coefficient_col, regression_usa, regression_dev)
    # coefficient_col = view(bootstrapped_outputs, :, i); regression_usa = boot_regression_usa; regression_dev = boot_regression_dev
    usa_propα = regression_usa.coef / regression_usa.coef[1]
    dev_propα = regression_dev.coef / regression_dev.coef[1]
    dev_m_usa = regression_dev.coef - regression_usa.coef
    dev_m_usa_prop = dev_propα - usa_propα
    
    coefficient_col .= [
        regression_usa.coef;
        regression_dev.coef;
        usa_propα;
        dev_propα;
        dev_m_usa;
        dev_m_usa_prop
    ]

    return coefficient_col
end

function _fill_coefficient_col_dated!(coefficient_col, regression_early, regression_late)
    # coefficient_col = view(bootstrapped_outputs, :, i); regression_early = boot_regression_early; regression_late = boot_regression_late
    early_propα = regression_early.coef / regression_late.coef[1]
    late_propα = regression_late.coef / regression_early.coef[1]
    late_m_early = regression_late.coef - regression_early.coef
    late_m_early_prop = late_propα - early_propα
    
    coefficient_col .= [
        regression_early.coef;
        regression_late.coef;
        early_propα;
        late_propα;
        late_m_early;
        late_m_early_prop
    ]

    return coefficient_col
end

function _fill_coefficient_col_curr!(coefficient_col, regression_domestic, regression_foreign)
    # coefficient_col = view(bootstrapped_outputs, :, i); regression_domestic = boot_regression_domestic; regression_foreign = boot_regression_foreign
    domestic_propα = regression_domestic.coef / regression_domestic.coef[1]
    foreign_propα = regression_foreign.coef / regression_foreign.coef[1]
    
    coefficient_col .= [
        regression_domestic.coef;
        regression_foreign.coef;
        domestic_propα;
        foreign_propα
    ]

    return coefficient_col
end

function _fill_coefficient_table!(coefficient_table, regression_usa, regression_dev)
    # regression_usa = boot_regression_usa; regression_dev = boot_regression_dev
    coefficient_table.usa_coef = regression_usa.coef
    coefficient_table.dev_coef = regression_dev.coef

    coefficient_table.usa_propα = coefficient_table.usa_coef / coefficient_table.usa_coef[1]
    coefficient_table.dev_propα = coefficient_table.dev_coef / coefficient_table.dev_coef[1]

    coefficient_table.dev_m_usa = coefficient_table.dev_coef - coefficient_table.usa_coef
    coefficient_table.dev_m_usa_prop = (
        coefficient_table.dev_propα - coefficient_table.usa_propα
    )

    return coefficient_table
end

function _fill_coefficient_table_curr!(
            coefficient_table, regression_domestic, regression_foreign
    )
    # regression_domestic = boot_regression_domestic; regression_foreign = boot_regression_foreign
    coefficient_table.domestic_coef = regression_domestic.coef
    coefficient_table.foreign_coef = regression_foreign.coef

    coefficient_table.domestic_propα = (
        coefficient_table.domestic_coef / coefficient_table.domestic_coef[1]
    )
    coefficient_table.foreign_propα = (
        coefficient_table.foreign_coef / coefficient_table.foreign_coef[1]
    )

    return coefficient_table
end

function _fill_coefficient_table_dated!(coefficient_table, regression_early, regression_late)
    # regression_early = boot_regression_early; regression_late = boot_regression_late
    coefficient_table.early_coef = regression_early.coef
    coefficient_table.late_coef = regression_late.coef

    coefficient_table.early_propα = coefficient_table.early_coef / coefficient_table.early_coef[1]
    coefficient_table.late_propα = coefficient_table.late_coef / coefficient_table.late_coef[1]

    coefficient_table.late_m_early = coefficient_table.late_coef - coefficient_table.early_coef
    coefficient_table.late_m_early_prop = (
        coefficient_table.late_propα - coefficient_table.early_propα
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
        sum_row
    )
    # usa_coef
    output.usa_coef[1:end-4] = _format_output_column(
        true_coefficient_table.usa_coef, true_se.usa_se
    )
    output.usa_coef[end-3:end] .= ""

    # dev_coef
    output.dev_coef[1:end-4] = _format_output_column(
        true_coefficient_table.dev_coef, true_se.dev_se
    )
    output.dev_coef[end-3:end] .= ""

    # usa_propα
    output.usa_propα[1:2] .= ""
    output.usa_propα[3:end-4] = _format_output_column(
        true_coefficient_table.usa_propα[2:end], bootstrapped_se.usa_propα[2:end];
        as_percent=true
    )
    output.usa_propα[end-3:end] .= ""

    # dev_propα
    output.dev_propα[1:2] .= ""
    output.dev_propα[3:end-4] = _format_output_column(
        true_coefficient_table.dev_propα[2:end], bootstrapped_se.dev_propα[2:end];
        as_percent=true
    )
    output.dev_propα[end-3:end] .= ""

    # dev_m_usa
    output.dev_m_usa[1:end-4] = _format_output_column(
        true_coefficient_table.dev_m_usa, bootstrapped_se.dev_m_usa
    )
    output.dev_m_usa[end-3:end-2] .= ""
    output.dev_m_usa[end-1:end] = _format_output_column(
        [sum_row[1, :dev_m_usa]], [sum_row[2, :dev_m_usa]]
    )

    # dev_m_usa_prop
    output.dev_m_usa_prop[1:2] .= ""
    output.dev_m_usa_prop[3:end-4] = _format_output_column(
        true_coefficient_table.dev_m_usa_prop[2:end], bootstrapped_se.dev_m_usa_prop[2:end];
        as_percent=true
    )
    output.dev_m_usa_prop[end-3:end-2] = _format_output_column(
        [sum_row[1, :dev_m_usa_prop_ff3]], [sum_row[2, :dev_m_usa_prop_ff3]];
        as_percent=true
    )
    output.dev_m_usa_prop[end-1:end] = _format_output_column(
        [sum_row[1, :dev_m_usa_prop]], [sum_row[2, :dev_m_usa_prop]]; as_percent=true
    )

    return output
end

function _fill_output_table_curr!(
        output,
        true_coefficient_table,
        bootstrapped_se,
        true_se
    )
    # domestic_coef
    output.domestic_coef = _format_output_column(
        true_coefficient_table.domestic_coef, true_se.domestic_se
    )

    # foreign_coef
    output.foreign_coef = _format_output_column(
        true_coefficient_table.foreign_coef, true_se.foreign_se
    )

    # domestic_propα
    output.domestic_propα = _format_output_column(
        true_coefficient_table.domestic_propα, bootstrapped_se.domestic_propα;
        as_percent=true
    )

    # foreign_propα
    output.foreign_propα = _format_output_column(
        true_coefficient_table.foreign_propα, bootstrapped_se.foreign_propα;
        as_percent=true
    )

    return output
end

function _fill_output_table_dated!(
    output,
    true_coefficient_table,
    bootstrapped_se,
    true_se,
    sum_row
)
# early_coef
output.early_coef[1] = _format_output_column(
    [true_coefficient_table.late_coef[1]], [true_se.late_se[1]]
)
output.early_coef[2:end-2] = _format_output_column(
    true_coefficient_table.early_coef[2:end], true_se.early_se[2:end]
)
output.early_coef[end-1:end] .= ""

# late_coef
output.late_coef[1] = _format_output_column(
    [true_coefficient_table.early_coef[1]], [true_se.early_se[1]]
)
output.late_coef[2:end-2] = _format_output_column(
    true_coefficient_table.late_coef[2:end], true_se.late_se[2:end]
)
output.late_coef[end-1:end] .= ""

# early_propα
output.early_propα[1:2] .= ""
output.early_propα[3:end-2] = _format_output_column(
    true_coefficient_table.early_propα[2:end], bootstrapped_se.early_propα[2:end];
    as_percent=true
)
output.early_propα[end-1:end] .= ""

# late_propα
output.late_propα[1:2] .= ""
output.late_propα[3:end-2] = _format_output_column(
    true_coefficient_table.late_propα[2:end], bootstrapped_se.late_propα[2:end];
    as_percent=true
)
output.late_propα[end-1:end] .= ""

# late_m_early
output.late_m_early[1:end-2] = _format_output_column(
    true_coefficient_table.late_m_early, bootstrapped_se.late_m_early
)
output.late_m_early[end-1:end] = _format_output_column(
    [sum_row[1, :late_m_early]], [sum_row[2, :late_m_early]]
)

# late_m_early_prop
output.late_m_early_prop[1:2] .= ""
output.late_m_early_prop[3:end-2] = _format_output_column(
    true_coefficient_table.late_m_early_prop[2:end], bootstrapped_se.late_m_early_prop[2:end];
    as_percent=true
)
output.late_m_early_prop[end-1:end] = _format_output_column(
    [sum_row[1, :late_m_early_prop]], [sum_row[2, :late_m_early_prop]]; as_percent=true
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