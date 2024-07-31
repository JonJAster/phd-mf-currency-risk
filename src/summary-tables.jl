using Revise
using DataFrames
using Arrow
using StatsBase
using Dates
using DataStructures
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function summary_tables()
    ## Panel A - Characteristics
    # Domestic Funds
    panela1 = _replicate_characteristics("Full Sample"; filter_by=x->!x.foreign)
    panela2 = _replicate_characteristics(
        "1991-2010";
        filter_by=x->(!x.foreign && x.date <= Date(2010,12,31))
    )
    panela3 = _replicate_characteristics(
        "2011-2023";
        filter_by=x->(!x.foreign && x.date >= Date(2011,1,1))
    )

    panela_domestic = vcat(panela1, panela2, panela3)

    println("Panel A - Characteristics")
    println("Domestic Funds")
    println()
    pprint(panela_domestic; centre=true)

    # Foreign Funds
    panela4 = _replicate_characteristics("Full Sample"; filter_by=x->x.foreign)
    panela5 = _replicate_characteristics(
        "1991-2010";
        filter_by=x->(x.foreign && x.date <= Date(2010,12,31))
    )
    panela6 = _replicate_characteristics(
        "2011-2023";
        filter_by=x->(x.foreign && x.date >= Date(2011,1,1))
    )

    panela_foreign = vcat(panela4, panela5, panela6)

    println("Foreign Funds")
    println()
    pprint(panela_foreign; centre=true)

    ## Panel B - Exposures
    # Domestic Funds - USA Factors
    panelb1 = _replicate_betas("Full Sample"; filter_by=x->!x.foreign, region="usa")
    panelb2 = _replicate_betas(
        "1991-2010";
        filter_by=x->(!x.foreign && x.date <= Date(2010,12,31)), region="usa"
    )
    panelb3 = _replicate_betas(
        "2011-2023";
        filter_by=x->(!x.foreign && x.date >= Date(2011,1,1)), region="usa"
    )

    panelb_domestic_usa = vcat(panelb1, panelb2, panelb3)

    println("Panel B - Exposures")
    println("Domestic Funds - USA Factors")
    println()
    pprint(panelb_domestic_usa; centre=true)

    # Domestic Funds - Developed Factors
    panelb4 = _replicate_betas("Full Sample"; filter_by=x->!x.foreign, region="dev")
    panelb5 = _replicate_betas(
        "1991-2010";
        filter_by=x->(!x.foreign && x.date <= Date(2010,12,31)), region="dev"
    )
    panelb6 = _replicate_betas(
        "2011-2023";
        filter_by=x->(!x.foreign && x.date >= Date(2011,1,1)), region="dev"
    )

    panelb_domestic_dev = vcat(panelb4, panelb5, panelb6)

    println("Domestic Funds - Developed Factors")
    println()
    pprint(panelb_domestic_dev; centre=true)

    # Foreign Funds - USA Factors
    panelb7 = _replicate_betas("Full Sample"; filter_by=x->x.foreign, region="usa")
    panelb8 = _replicate_betas(
        "1991-2010";
        filter_by=x->(x.foreign && x.date <= Date(2010,12,31)), region="usa"
    )
    panelb9 = _replicate_betas(
        "2011-2023";
        filter_by=x->(x.foreign && x.date >= Date(2011,1,1)), region="usa"
    )

    panelb_foreign_usa = vcat(panelb7, panelb8, panelb9)

    println("Foreign Funds - USA Factors")
    println()
    pprint(panelb_foreign_usa; centre=true)

    # Foreign Funds - Developed Factors
    panelb10 = _replicate_betas("Full Sample"; filter_by=x->x.foreign, region="dev")
    panelb11 = _replicate_betas(
        "1991-2010";
        filter_by=x->(x.foreign && x.date <= Date(2010,12,31)), region="dev"
    )
    panelb12 = _replicate_betas(
        "2011-2023";
        filter_by=x->(x.foreign && x.date >= Date(2011,1,1)), region="dev"
    )

    panelb_foreign_dev = vcat(panelb10, panelb11, panelb12)

    println("Foreign Funds - Developed Factors")
    println()
    pprint(panelb_foreign_dev; centre=true)

    ## Panel C - Return Components
    # Domestic Funds - USA Factors
    panelc1 = _replicate_return_components(
        "Full Sample";
        filter_by=x->!x.foreign, region="usa"
    )
    panelc2 = _replicate_return_components(
        "1991-2010";
        filter_by=x->(!x.foreign && x.date <= Date(2010,12,31)), region="usa"
    )
    panelc3 = _replicate_return_components(
        "2011-2023";
        filter_by=x->(!x.foreign && x.date >= Date(2011,1,1)), region="usa"
    )

    panelc_domestic_usa = vcat(panelc1, panelc2, panelc3)

    println("Panel C - Return Components")
    println("Domestic Funds - USA Factors")
    println()
    pprint(panelc_domestic_usa; centre=true)

    # Domestic Funds - Developed Factors
    panelc4 = _replicate_return_components(
        "Full Sample";
        filter_by=x->!x.foreign, region="dev"
    )
    panelc5 = _replicate_return_components(
        "1991-2010";
        filter_by=x->(!x.foreign && x.date <= Date(2010,12,31)), region="dev"
    )
    panelc6 = _replicate_return_components(
        "2011-2023";
        filter_by=x->(!x.foreign && x.date >= Date(2011,1,1)), region="dev"
    )

    panelc_domestic_dev = vcat(panelc4, panelc5, panelc6)

    println("Domestic Funds - Developed Factors")
    println()
    pprint(panelc_domestic_dev; centre=true)

    # Foreign Funds - USA Factors
    panelc7 = _replicate_return_components(
        "Full Sample";
        filter_by=x->x.foreign, region="usa"
    )
    panelc8 = _replicate_return_components(
        "1991-2010";
        filter_by=x->(x.foreign && x.date <= Date(2010,12,31)), region="usa"
    )
    panelc9 = _replicate_return_components(
        "2011-2023";
        filter_by=x->(x.foreign && x.date >= Date(2011,1,1)), region="usa"
    )

    panelc_foreign_usa = vcat(panelc7, panelc8, panelc9)

    println("Foreign Funds - USA Factors")
    println()
    pprint(panelc_foreign_usa; centre=true)

    # Foreign Funds - Developed Factors
    panelc10 = _replicate_return_components(
        "Full Sample";
        filter_by=x->x.foreign, region="dev"
    )
    panelc11 = _replicate_return_components(
        "1991-2010";
        filter_by=x->(x.foreign && x.date <= Date(2010,12,31)), region="dev"
    )
    panelc12 = _replicate_return_components(
        "2011-2023";
        filter_by=x->(x.foreign && x.date >= Date(2011,1,1)), region="dev"
    )

    panelc_foreign_dev = vcat(panelc10, panelc11, panelc12)

    println("Foreign Funds - Developed Factors")
    println()
    pprint(panelc_foreign_dev; centre=true)
end

function _replicate_characteristics(sample_period; filter_by=nothing)
    # label_head = "Sample Period"; label = "Full Sample"; filter_by = x->!x.foreign
    data_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")
    
    data = loadarrow(data_filename)

    !isnothing(filter_by) && filter!(filter_by, data)

    summary_parameters = OrderedDict(
        :flow => "Flow (%)",
        :net_assets_m1 => "Size (\$mil)",
        :costs => "Annualised Expenses (%)",
        :no_load => "% No Load",
        :age => "Age (months)",
        :ex_ret => "Excess Return (%)",
        :std_return_12m => "12-Month Return Volatility (%)",
        :usa_correlation_12m => "12-Month US Correlation"
    )

    data[!, summary_parameters[:flow]] = data.flow * 100
    data[!, summary_parameters[:net_assets_m1]] = data.net_assets_m1 / 1_000_000
    data[!, summary_parameters[:costs]] = data.costs * 100 * 12
    data[!, summary_parameters[:no_load]] = data.no_load
    data[!, summary_parameters[:age]] = data.age
    data[!, summary_parameters[:ex_ret]] = data.ex_ret * 100
    data[!, summary_parameters[:std_return_12m]] = data.std_return_12m * 100
    data[!, summary_parameters[:usa_correlation_12m]] = data.usa_correlation_12m

    output_characteristics = Dict(
        Symbol("Sample Period") => sample_period,
        :n => nrow(data)
    ) |> DataFrame
    for i in keys(summary_parameters)
        output_characteristics = hcat(
            output_characteristics,
            _summarise_series(data[!, [summary_parameters[i]]])
        )
    end

    return output_characteristics
end

function _replicate_betas(sample_period; filter_by=nothing, region)
    # sample_period = "Full Sample"; filter_by = x->!x.foreign; region = "usa"
    factor_set = "ff_$(region)_ffc6.arrow"
    
    data_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")
    return_beta_filename = joinpath(DIRS.combo.return_betas, factor_set)
    
    data = loadarrow(data_filename)
    return_betas = loadarrow(return_beta_filename)

    return_betas = unstack(return_betas, [:fundid, :date], :factor, :coef)

    return_betas_filt = innerjoin(
        data,
        return_betas,
        on=[:fundid, :date]
    )

    !isnothing(filter_by) && filter!(filter_by, return_betas_filt)
    select!(return_betas_filt, Not(propertynames(data)))
    
    dropmissing!(return_betas_filt)

    betas_parameters = OrderedDict(
        :const => "Alpha",
        :mkt => "Beta",
        :smb => "Size coefficient",
        :hml => "Value coefficient",
        :wml => "Momentum coefficient",
        :rmw => "Profitability coefficient",
        :cma => "Investment coefficient"
    )

    return_betas_filt[!, betas_parameters[:const]] = return_betas_filt.const * 100
    return_betas_filt[!, betas_parameters[:mkt]] = return_betas_filt.mkt
    return_betas_filt[!, betas_parameters[:smb]] = return_betas_filt.smb
    return_betas_filt[!, betas_parameters[:hml]] = return_betas_filt.hml
    return_betas_filt[!, betas_parameters[:wml]] = return_betas_filt.wml
    return_betas_filt[!, betas_parameters[:rmw]] = return_betas_filt.rmw
    return_betas_filt[!, betas_parameters[:cma]] = return_betas_filt.cma

    output_betas = Dict(
        Symbol("Sample Period") => sample_period,
        :n => nrow(data)
    ) |> DataFrame
    for i in keys(betas_parameters)
        output_betas = hcat(
            output_betas,
            _summarise_series(return_betas_filt[!, [betas_parameters[i]]])
        )
    end

    return output_betas
end

function _replicate_return_components(sample_period; filter_by=nothing, region)
    factor_set = "ff_$(region)_ffc6.arrow"

    mf_data_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")
    return_components_filename = joinpath(DIRS.combo.weighted, factor_set)
    
    mf_data = loadarrow(mf_data_filename)
    return_components = loadarrow(return_components_filename)

    transform!(
        groupby(mf_data, :fundid),
        :ex_ret => (x -> lag(x, 1)) => :ex_ret_m1
    )

    return_components_filt = innerjoin(
        return_components,
        mf_data,
        on=[:fundid, :date]
    )

    !isnothing(filter_by) && filter!(filter_by, return_components_filt)
    select!(return_components_filt, :ex_ret_m1, Not(propertynames(mf_data)))

    return_parameters = OrderedDict(
        :ex_ret => "Excess Return (b.p.)",
        :ret_alpha => "wret_alpha (b.p.)",
        :ret_mkt => "wret_mkt (b.p.)",
        :ret_smb => "wret_smb (b.p.)",
        :ret_hml => "wret_hml (b.p.)",
        :ret_wml => "wret_wml (b.p.)",
        :ret_rmw => "wret_rmw (b.p.)",
        :ret_cma => "wret_cma (b.p.)"
    )

    return_components_filt[!, return_parameters[:ex_ret]] = return_components_filt.ex_ret_m1 * 10000
    return_components_filt[!, return_parameters[:ret_alpha]] = return_components_filt.ret_alpha_m1 * 10000
    return_components_filt[!, return_parameters[:ret_mkt]] = return_components_filt.ret_mkt_m1 * 10000
    return_components_filt[!, return_parameters[:ret_smb]] = return_components_filt.ret_smb_m1 * 10000
    return_components_filt[!, return_parameters[:ret_hml]] = return_components_filt.ret_hml_m1 * 10000
    return_components_filt[!, return_parameters[:ret_wml]] = return_components_filt.ret_wml_m1 * 10000
    return_components_filt[!, return_parameters[:ret_rmw]] = return_components_filt.ret_rmw_m1 * 10000
    return_components_filt[!, return_parameters[:ret_cma]] = return_components_filt.ret_cma_m1 * 10000

    output_return_components = Dict(
        Symbol("Sample Period") => sample_period,
        :n => nrow(mf_data)
    ) |> DataFrame
    for i in keys(return_parameters)
        output_return_components = hcat(
            output_return_components,
            _summarise_series(return_components_filt[!, [return_parameters[i]]])
        )
    end

    return output_return_components
end

function _summarise_series(data_series)
    # data_series = data[!, [summary_parameters[collect(keys(summary_parameters))[3]]]]
    
    parameter_name = first(propertynames(data_series))
    data = skipmissing(data_series[!, parameter_name]) |> collect

    data_mean = round(mean(data), digits=2)
    data_median = round(median(data), digits=2)
    data_sd = round(std(data), digits=2)

    # Round according to the larger of the mean and median
    string_digits = minimum(_pretty_decimals_count.([data_mean, data_median]))
    mean_str = _pretty_string(data_mean, string_digits)
    median_str = _pretty_string(data_median, string_digits)
    sd_str = _pretty_string(data_sd, string_digits)

    output = DataFrame(
        Dict(parameter_name => """$mean_str | $median_str ($sd_str)""")
    )

    return output
end

function _pretty_decimals_count(num)
    # num = 1.0
    test_round = round(abs(num), digits=2) |> string
    num_halves = split(test_round, ".")
    left_digits = length(num_halves[1])

    left_digits >= 3 && return 0
    left_digits == 2 && return 1
    left_digits == 1 && return 2

    error("Unexpected number of digits")
    return nothing
end

function _pretty_string(num, printed_digits)
    # num = 1.0; printed_digits = 2
    test_round = round(num, digits=2)
    test_round == 0 && return "0.00"
    rounded_string = round(num, digits=2) |> string
    num_halves = split(rounded_string, ".")
    right_digits = length(num_halves[2])

    if printed_digits == 0
        # 412
        output = round(num, digits=0) |> Int |> string
    elseif printed_digits == 1
        # 12.1
        output = round(num, digits=1) |> string
    elseif printed_digits == 2
        # 4.82
        output = round(num, digits=2) |> string

        # 9.10
        right_digits == 1 && (output *= "0")
    else
        error("Unexpected number of digits")
    end

    return output
end

if isnothing(match(r"terminalserver.jl$", PROGRAM_FILE))
    summary_tables()
end

