using DataFrames
using Arrow
using ShiftedArrays: lag
using StatsBase
using Plots

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "post-processing")
const LOG_SERIES = ["fund_assets", "fund_flow_dollars"]
const UNITS = Dict(
    :ret => "%",
    :fund_flow => "%",
    :fund_flow_dollars => "\$1M USD",
    :fund_flow_percent => "%",
    :fund_assets => "\$1M USD",
    :mean_costs => "%",
)

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    filename = joinpath(INPUT_DIR, options_folder, "main/fund_data.arrow")
    
    df_mf = Arrow.Table(filename) |> DataFrame

    df_usa = df_mf[df_mf.domicile .== "USA", :]

    display_summary(df_usa)
end

function display_summary(df)
    df_data = copy(df[:, [:ret, :fund_flow, :fund_assets, :mean_costs]])

    dropmissing!(df_data, [:ret, :fund_flow])

    df_data[!, :fund_assets] = df_data.fund_assets ./ 1e6
    df_data[!, :fund_flow_dollars] = df_data.fund_flow .* lag(df_data.fund_assets)

    rename!(df_data, :fund_flow => :fund_flow_percent)
    
    summary_table(df_data)
    summary_distributions(df_data)
end

function summary_table(df)
    df_summary = DataFrame(
        :series => Symbol[],
        :units => String[],
        :mean => Float64[],
        :median => Float64[],
        :std => Float64[],
        :skew => Float64[],
        :min => Float64[],
        :max => Float64[],
        :n_obs => Int[]
    )

    for series in names(df)
        series_data = skipmissing(df[!, series]) |> collect
        series_name = Symbol(series)
        push!(df_summary, [
            series_name,
            UNITS[series_name],
            mean(series_data),
            median(series_data),
            std(series_data),
            skewness(series_data),
            minimum(series_data),
            maximum(series_data),
            length(series_data)
        ])
    end

    display(df_summary)
end

function summary_distributions(df)
    for series in names(df)
        series == "fund_flow_dollars" && continue
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

main("usd-rets_na-int_eq-strict_targets_age-filtered")