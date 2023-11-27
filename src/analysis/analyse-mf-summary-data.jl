using DataFrames
using Arrow
using ShiftedArrays: lag
using StatsBase
using Plots

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("shared/DataInit.jl")
using .CommonFunctions
using .CommonConstants
using .DataInit

const INPUT_DIR = joinpath(DIRS.fund, "post-processing")
const LOG_SERIES = ["fund_size"]
const UNITS = Dict(
    :percentage_fund_flow => "%",
    :fund_size => "\$1M USD",
    :fund_age => "months",
    :expense_ratio => "%",
    :load_fund_dummy => "0/1",
    :volatility_lag12_to_lag1 => "%",
)
const SUMMARY_COLS = [
    :percentage_fund_flow,
    :fund_size,
    :fund_age,
    :expense_ratio,
    :load_fund_dummy,
    :volatility_lag12_to_lag1,
]

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    df_usa = initialise_flow_data(options_folder, COMPLETE_MODELS[1]; ret=:weighted)
    
    display_summary(df_usa)
end

function display_summary(df)
    df_data = copy(df)

    rename!(df_data, :fund_flow => :percentage_fund_flow)
    df_data[!, :fund_size] = exp.(df_data.log_size) ./ 1e6
    df_data[!, :fund_age] = exp.(df_data.log_age)
    rename!(df_data, :mean_costs => :expense_ratio)
    df_data[!, :load_fund_dummy] = .!(df_data.no_load)
    rename!(df_data, :std_return_12m => :volatility_lag12_to_lag1)
    
    dropmissing!(df_data, SUMMARY_COLS)

    df_summarydata = select!(df_data, SUMMARY_COLS)

    summary_table(df_summarydata)
    summary_distributions(df_summarydata)
end

function summary_table(df)
    df_summary = DataFrame(
        :series => Symbol[],
        :units => String[],
        :n_obs => Int[],
        :mean => Float64[],
        :std => Float64[],
        :p25 => Float64[],
        :median => Float64[],
        :p75 => Float64[]
    )

    for series in names(df)
        series_data = skipmissing(df[!, series]) |> collect
        series_name = Symbol(series)
        push!(df_summary, [
            series_name,
            UNITS[series_name],
            length(series_data),
            mean(series_data),
            std(series_data),
            percentile(series_data, 0.25),
            median(series_data),
            percentile(series_data, 0.75)
        ])
    end

    display(df_summary)
end

function summary_distributions(df)
    for series in names(df)
        series_units = UNITS[Symbol(series)]
        if series in LOG_SERIES
            series_data = skipmissing(df[!, series]) |> collect .|> log
            series = "log[$series]"
        else
            series_data = skipmissing(df[!, series]) |> collect
        end
        histogram(
            series_data,
            title=series,
            label=false,
            linewidth=0,
            normalize=:probability,
            ylab="Proportion of Observations (%)",
            xlab="$series ($series_units)",
            xlim=(percentile(series_data, 0.0001), percentile(series_data, 99.99))
        ) |> display
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end