using
    DataFrames,
    Arrow,
    GLM

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("shared/DataInit.jl")
using
    .CommonFunctions,
    .CommonConstants,
    .DataInit

const OUTPUT_DIR = joinpath(DIRS.fund, "post-processing")

const OFFSET_FOR_CONSTANT = 1

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    start_time = time()

    output_folder = joinpath(OUTPUT_DIR, options_folder, "flow-betas")

    savelock = ReentrantLock()
    for model in COMPLETE_MODELS
        process_start = time()
        model_name = name_model(model)
        flow_data = initialise_flow_data(options_folder, model; ret=:weighted)

        cols = names(flow_data)
        find_return_col(name) = !isnothing(match(r"ret_", name))
        return_component_cols = cols[find_return_col.(cols)] .|> Symbol

        regression_data = regression_table(
            flow_data, :fundid, :date,
            :fund_flow, :plus_lag, 19,
            return_component_cols...,
            :mean_costs, :lag, 19,
            :no_load,
            :std_return_12m,
            :log_size, :lag,
            :log_age, :lag,
            :tfe, :month
        )
        
        dropmissing!(regression_data)
        drop_zero_cols!(regression_data)

        flow_betas = flow_regression(regression_data, return_component_cols)

        lock(savelock) do
            output_filename = makepath(output_folder, "$model_name.arrow")
            Arrow.write(flow_betas, output_filename)
        end
    end
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
    for col in propertynames(data)
        all(data[!, col] .== 0) && push!(zero_cols, col)
    end
    
    select!(data, Not(zero_cols))
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

# AUTO TESTS
options_folder=option_foldername(; DEFAULT_OPTIONS...)
model = (COMPLETE_MODELS)[4]
using Dates
using StatsBase

# MANUAL TESTS
if false
    data = regression_data
    return_cols = 1

    for col in names(regression_data)
        !(eltype(regression_data[!, col]) <: Number) && continue
        println("sum of $col: ", sum(regression_data[!, col]))
    end
    
    findfirst(<=(-100000), regression_data.log_age_lag1)

    regression_data[480910:480915,[:fundid, :date, :log_age_lag1]]

    info = Arrow.Table(joinpath(DIRS.fund, "info/mf_info.arrow")) |> DataFrame
    info[info.fundid .== "FSUSA002PR", :] |> Matrix |> vec |> println

    rowdate = Date(2016,11,30)
    incdate = Date(1987,12,21)
    age = (
        12*(year(rowdate) .- year(incdate)) .+
        (month(rowdate) .- month(incdate)) .+ 1
    )
end