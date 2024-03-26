using Revise
using DataFrames
using Arrow
using StatsBase
using DataStructures

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function replicate_bho_summary_table()
    output_characteristics = _replicate_characteristics()
    output_betas = _replicate_betas()
    output_return_components = _replicate_return_components()
end

function _replicate_characteristics(start_month=nothing, end_month=nothing)
    regression_packet = (
        flow_regression_table("usa_ff3"; filter_by=x->investment_target_is(x, :usa))
    )
    
    main_data = regression_packet.regression_data

    summary_parameters = OrderedDict(
        :flow => "Percentage fund flow",
        :log_lag_size => "Fund size (\$mil)",
        :log_age_lag1 => "Fund age (months)",
        :costs_lag1 => "Expense ratio",
        :true_no_load => "Load fund dummy",
        :std_return_12m => "Volatility (t-12 to t-1)"
    )

    main_data[!, summary_parameters[:flow]] = main_data.flow*100
    main_data[!, summary_parameters[:log_lag_size]] = (ℯ .^ (main_data.log_lag_size)) / 1_000_000
    main_data[!, summary_parameters[:log_age_lag1]] = ℯ .^ (main_data.log_age_lag1) 
    main_data[!, summary_parameters[:costs_lag1]] = main_data.costs_lag1 * 100
    main_data[!, summary_parameters[:true_no_load]] = main_data.true_no_load
    main_data[!, summary_parameters[:std_return_12m]] = main_data.std_return_12m * 100

    output_characteristics = _summarise_series(main_data[!, [summary_parameters[:flow]]])
    for i in keys(summary_parameters)
        i == :flow && continue
        
        output_characteristics = vcat(
            output_characteristics,
            _summarise_series(main_data[!, [summary_parameters[i]]])
        )
    end

    return output_characteristics
end

function _replicate_betas(start_month=nothing, end_month=nothing)
    return_beta_filename = joinpath(DIRS.combo.return_betas, "usa_ff3.arrow")
    
    return_betas = loadarrow(return_beta_filename)

    return_betas = unstack(return_betas, [:fundid, :date], :factor, :coef)
    dropmissing!(return_betas)

    betas_parameters = OrderedDict(
        :const => "Alpha",
        :mkt => "Beta",
        :smb => "Size coefficient",
        :hml => "Value coefficient"
    )

    return_betas[!, betas_parameters[:const]] = return_betas.const * 100
    return_betas[!, betas_parameters[:mkt]] = return_betas.mkt
    return_betas[!, betas_parameters[:smb]] = return_betas.smb
    return_betas[!, betas_parameters[:hml]] = return_betas.hml

    output_betas = _summarise_series(return_betas[!, [betas_parameters[:const]]])
    for i in keys(betas_parameters)
        i == :const && continue

        output_betas = vcat(
            output_betas,
            _summarise_series(return_betas[!, [betas_parameters[i]]])
        )
    end

    return output_betas
end

function _replicate_return_components(start_month=nothing, end_month=nothing)
    return_components_filename = joinpath(DIRS.combo.weighted, "usa_ff3.arrow")

    return_components = loadarrow(return_data_filename)

    return_parameters = OrderedDict(
        :ret_alpha => "ALPHA",
        :ret_mkt => "MKTRET",
        :ret_smb => "SIZRET",
        :ret_hml => "VALRET"
    )

    return_components[!, return_parameters[:ret_alpha]] = return_components.ret_alpha * 100
    return_components[!, return_parameters[:ret_mkt]] = return_components.ret_mkt * 100
    return_components[!, return_parameters[:ret_smb]] = return_components.ret_smb * 100
    return_components[!, return_parameters[:ret_hml]] = return_components.ret_hml * 100

    return_components = combine(
        groupby(return_components, :date),
        return_parameters[:ret_alpha] => mean => return_parameters[:ret_alpha],
        return_parameters[:ret_mkt] => mean => return_parameters[:ret_mkt],
        return_parameters[:ret_smb] => mean => return_parameters[:ret_smb],
        return_parameters[:ret_hml] => mean => return_parameters[:ret_hml]
    )

    output_return_components = (
        _summarise_series(return_components[!, [return_parameters[:ret_alpha]]])
    )
    for i in keys(return_parameters)
        i == :ret_alpha && continue

        output_return_components = vcat(
            output_return_components,
            _summarise_series(return_components[!, [return_parameters[i]]])
        )
    end

    return output_return_components
end

function _summarise_series(data_series)
    parameter_name = first(propertynames(data_series))
    data = skipmissing(data_series[!, parameter_name]) |> collect

    output = DataFrame(
        parameter_name = [parameter_name],
        num_obs = [length(data)],
        mean = [mean(data) |> x-> round(x, digits=3)],
        sd = [std(data) |> x-> round(x, digits=3)],
        p25 = [quantile(data, 0.25) |> x-> round(x, digits=3)],
        median = [median(data) |> x-> round(x, digits=3)],
        p75 = [quantile(data, 0.75) |> x-> round(x, digits=3)]
    )

    return output
end

if abspath(PROGRAM_FILE) == @__FILE__
    replicate_bho_summary_table()
end