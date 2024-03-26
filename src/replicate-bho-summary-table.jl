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

function replicate_bho_summary_table()
    return_beta_filename = joinpath(DIRS.combo.return_betas, "usa_ff3.arrow")
    
    return_betas = loadarrow(return_beta_filename)
    main_data = flow_regression_table("usa_ff3"; filter_by=x->investment_target_is(x, :usa))

    main_data.lag_size = ℯ .^ (main_data.log_lag_size)
    main_data.age = ℯ .^ (main_data.log_age)
    output_characteristics = _summarise_series(main_data[!, [:flow]])
    for i in [:lag_size, :age, :costs, :true_no_load, :std_return_12m]
        output_characteristics = vcat(output_characteristics, _summarise_series(main_data[!, [i]]))
    end

    output_characteristics
end

function _summarise_series(data_series)
    parameter_name = first(propertynames(data_series))
    data = skipmissing(data_series[!, parameter_name]) |> collect

    output = DataFrame(
        parameter_name = [parameter_name],
        num_obs = [length(data)],
        mean = [mean(data)],
        sd = [std(data)],
        p25 = [quantile(data, 0.25)],
        median = [median(data)],
        p75 = [quantile(data, 0.75)]
    )

    return output
end

if abspath(PROGRAM_FILE) == @__FILE__
    replicate_bho_summary_table()
end