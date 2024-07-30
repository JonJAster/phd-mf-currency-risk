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
    _replicate_characteristics()
    _replicate_characteristics(end_month=Date(2010,12,31))
    _replicate_characteristics(start_month=Date(2011,1,1))

    _replicate_betas()
    _replicate_betas(end_month=Date(2010,12,31))
    _replicate_betas(start_month=Date(2011,1,1))

    _replicate_betas(region="dev")
    _replicate_betas(end_month=Date(2010,12,31), region="dev")
    _replicate_betas(start_month=Date(2011,1,1), region="dev")

    _replicate_return_components()
    _replicate_return_components(end_month=Date(2010,12,31))
    _replicate_return_components(start_month=Date(2011,1,1))

    _replicate_return_components(region="dev")
    _replicate_return_components(end_month=Date(2010,12,31), region="dev")
    _replicate_return_components(start_month=Date(2011,1,1), region="dev")
end

function _replicate_characteristics(filter_by=nothing)
    data_filename = joinpath(DIRS.mf.refined, "mf-excess-returns.arrow")
    info_filename = joinpath(DIRS.mf.refined, "mf-info.arrow")
    
    data = loadarrow(data_filename)
    info = loadarrow(info_filename)

    data = innerjoin(data, info[!,[:fundid, :inception_date, :true_no_load]], on=:fundid)

    !isnothing(filter_by) && data = filter(filter_by, data)

    data.age = (
        12 .* (year.(data.date) .- year.(data.inception_date))
        .+ month.(data.date) .- month.(data.inception_date)
    )

    data.std_return_12m = rolling_std(data, :ex_ret, 12; lagged=true, grouped_by=:fundid)

    summary_parameters = OrderedDict(
        :flow => "Flow",
        :net_assets_m1 => "Size (\$mil)",
        :age => "Age (months)",
        :costs => "Expense Ratio",
        :true_no_load => "% No Load",
        :ex_ret => "Monthly Excess Return",
        :std_return_12m => "12-Month Return Volatility"
    )

    data[!, summary_parameters[:flow]] = data.flow * 100
    data[!, summary_parameters[:net_assets_m1]] = data.net_assets_m1 / 1_000_000
    data[!, summary_parameters[:age]] = data.age
    data[!, summary_parameters[:costs]] = data.costs * 100 * 12
    data[!, summary_parameters[:true_no_load]] = data.true_no_load
    data[!, summary_parameters[:ex_ret]] = data.ex_ret * 100
    data[!, summary_parameters[:std_return_12m]] = data.std_return_12m * 100

    output_characteristics = _summarise_series(data[!, [summary_parameters[:flow]]])
    for i in keys(summary_parameters)
        i == :flow && continue
        
        output_characteristics = vcat(
            output_characteristics,
            _summarise_series(data[!, [summary_parameters[i]]])
        )
    end

    return output_characteristics
end

function _replicate_betas(;start_month=nothing, end_month=nothing, region="usa")
    factor_set = "ff_$(region)_ffc6.arrow"
    return_beta_filename = joinpath(DIRS.combo.return_betas, factor_set)
    
    return_betas = loadarrow(return_beta_filename)

    if !isnothing(start_month)
        return_betas = return_betas[return_betas.date .>= start_month, :]
    end

    if !isnothing(end_month)
        return_betas = return_betas[return_betas.date .<= end_month, :]
    end

    return_betas = unstack(return_betas, [:fundid, :date], :factor, :coef)
    dropmissing!(return_betas)

    betas_parameters = OrderedDict(
        :const => "Alpha",
        :mkt => "Beta",
        :smb => "Size coefficient",
        :hml => "Value coefficient",
        :wml => "Momentum coefficient",
        :rmw => "Profitability coefficient",
        :cma => "Investment coefficient"
    )

    return_betas[!, betas_parameters[:const]] = return_betas.const * 100
    return_betas[!, betas_parameters[:mkt]] = return_betas.mkt
    return_betas[!, betas_parameters[:smb]] = return_betas.smb
    return_betas[!, betas_parameters[:hml]] = return_betas.hml
    return_betas[!, betas_parameters[:wml]] = return_betas.wml
    return_betas[!, betas_parameters[:rmw]] = return_betas.rmw
    return_betas[!, betas_parameters[:cma]] = return_betas.cma

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

function _replicate_return_components(;start_month=nothing, end_month=nothing, region="usa")
    factor_set = "ff_$(region)_ffc6.arrow"
    return_components_filename = joinpath(DIRS.combo.weighted, factor_set)
    mf_data_filename = joinpath(DIRS.mf.refined, "mf-excess-returns.arrow")

    return_components = loadarrow(return_components_filename)
    mf_data = loadarrow(mf_data_filename)

    transform!(
        groupby(mf_data, :fundid),
        :ex_ret => (x -> lag(x, 1)) => :ex_ret_m1
    )

    return_components = innerjoin(
        return_components,
        mf_data[!, [:fundid, :date, :ex_ret_m1]],
        on=[:fundid, :date]
    )

    if !isnothing(start_month)
        return_components = return_components[return_components.date .>= start_month, :]
    end

    if !isnothing(end_month)
        return_components = return_components[return_components.date .<= end_month, :]
    end

    return_parameters = OrderedDict(
        :ex_ret => "Monthly Excess Return",
        :ret_alpha => "wret_alpha",
        :ret_mkt => "wret_mkt",
        :ret_smb => "wret_smb",
        :ret_hml => "wret_hml",
        :ret_wml => "wret_wml",
        :ret_rmw => "wret_rmw",
        :ret_cma => "wret_cma"
    )

    return_components[!, return_parameters[:ex_ret]] = return_components.ex_ret_m1 * 10000
    return_components[!, return_parameters[:ret_alpha]] = return_components.ret_alpha_m1 * 10000
    return_components[!, return_parameters[:ret_mkt]] = return_components.ret_mkt_m1 * 10000
    return_components[!, return_parameters[:ret_smb]] = return_components.ret_smb_m1 * 10000
    return_components[!, return_parameters[:ret_hml]] = return_components.ret_hml_m1 * 10000
    return_components[!, return_parameters[:ret_wml]] = return_components.ret_wml_m1 * 10000
    return_components[!, return_parameters[:ret_rmw]] = return_components.ret_rmw_m1 * 10000
    return_components[!, return_parameters[:ret_cma]] = return_components.ret_cma_m1 * 10000

    output_return_components = (
        _summarise_series(return_components[!, [return_parameters[:ex_ret]]])
    )

    for i in keys(return_parameters)
        i == :ex_ret && continue

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
        mean = [mean(data) |> x-> round(x, digits=2)],
        median = [median(data) |> x-> round(x, digits=2)],
        sd = [std(data) |> x-> round(x, digits=2)]
    )

    return output
end

if !isnothing(match(r"terminalserver.jl", PROGRAM_FILE))
    summary_tables()
end