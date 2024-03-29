using DataFrames
using Arrow
using GLM
using Base.Threads

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("shared/DataInit.jl")
using
    .CommonFunctions,
    .CommonConstants,
    .DataInit

const OUTPUT_DIR = joinpath(DIRS.fund, "post-processing")

const MAX_LAGS = 19

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    time_start = time()

    output_folder = joinpath(OUTPUT_DIR, options_folder, "flow-betas")

    savelock = ReentrantLock()
    @threads for model in COMPLETE_MODELS
        process_start = time()
        model_name = name_model(model)
        flow_data = initialise_flow_data(options_folder, model; ret=:weighted)

        cols = names(flow_data)
        find_return_col(name) = !isnothing(match(r"ret_", name))
        return_component_cols = cols[find_return_col.(cols)] .|> Symbol

        regression_data = regression_table(
            flow_data, :fundid, :date,
            :fund_flow, :plus_lag, MAX_LAGS,
            return_component_cols...,
            :mean_costs, :lag, MAX_LAGS,
            :no_load,
            :std_return_12m,
            :log_size, :lag,
            :log_age, :lag,
            :tfe, :month
        )
        
        dropmissing!(regression_data)
        drop_zero_cols!(regression_data)

        # TODO: This makes much more sense to be done in the step of initialising flow
        # data, but when doing it that way the coefficients on alpha are not scaled as
        # they should be. I'll check back later as to why and refactor this into a more
        # appropriate place.
        regression_data.fund_flow .*= 100

        flow_betas = flow_regression(regression_data, return_component_cols)

        lock(savelock) do
            output_filename = makepath(output_folder, "$model_name.arrow")
            Arrow.write(output_filename, flow_betas)
        end
        
        process_elapsed_s = round(time() - process_start, digits=2)
        process_elapsed_m = round(process_elapsed_s/60, digits=2)
        println(
            "Process finished regressing on $model_name with " *
            " in $process_elapsed_s seconds " *
            "($process_elapsed_m minutes)"
        )
    end

    time_duration_s = round(time() - time_start, digits=2)
    time_duration_m = round(time_duration_s/60, digits=2)
    println(
        "Finished flow regressions in $time_duration_s seconds " *
        "($time_duration_m minutes)"
    )
end

function flow_regression(data, return_cols)
    y = data.fund_flow
    n_obs = length(y)
    
    X_cols = setdiff(propertynames(data), [:fundid, :date, :fund_flow])
    X_no_constant = Matrix(data[!, X_cols])
    X = hcat(ones(n_obs), X_no_constant)
    
    regfit = lm(X, y)
    return_col_indices = findall(in(return_cols), X_cols)

    flow_betas = DataFrame(
        factor = return_cols,
        coef = coef(regfit)[return_col_indices],
        se = stderror(regfit)[return_col_indices]
    )

    df = n_obs - size(X, 2)
    flow_betas.tstat = flow_betas.coef ./ flow_betas.se

    return flow_betas
end

function drop_zero_cols!(data)
    zero_cols = []
    for col in names(data)
        all(data[!, col] .== 0) && push!(zero_cols, col)
    end
    
    if length(zero_cols) != MAX_LAGS
        println(
            "Warning: MAX_LAGS is $MAX_LAGS, but " *
            "found $(length(zero_cols)) zero columns to drop."
        )
    end
    select!(data, Not(zero_cols))
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end