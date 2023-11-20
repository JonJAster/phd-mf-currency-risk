using
    DataFrames,
    Arrow,
    Base.Threads

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("shared/DataInit.jl")
using
    .CommonFunctions,
    .CommonConstants,
    .DataInit

const INPUT_DIR = joinpath(DIRS.fund, "post-processing")
const OUTPUT_DIR = INPUT_DIR

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    time_start = time()

    main_data = initialise_base_data(options_folder)
    betas_folderstring = joinpath(INPUT_DIR, options_folder, "factor-betas")
    output_folderstring = joinpath(OUTPUT_DIR, options_folder, "decompositions")

    savelock = ReentrantLock()
    @threads for model in COMPLETE_MODELS
        process_start = time()
        model_name = name_model(model)
        betas_filestring = joinpath(betas_folderstring, "$model_name.arrow")
        regression_outputs = Arrow.Table(betas_filestring) |> DataFrame

        betas = extract_betas(regression_outputs)
        full_data = innerjoin(main_data, betas, on=[:fundid, :date])

        decomposed_returns = decompose_returns(full_data, model)
        
        save_decomposition(decomposed_returns, output_folderstring, model_name, savelock)

        process_elapsed_s = round(time() - process_start, digits=2)
        process_elapsed_m = round(process_elapsed_s/60, digits=2)
        println(
            "Process finished decomposing returns on $(model_name) in " *
            "$process_elapsed_s seconds ($process_elapsed_m minutes)"
        )
    end

    time_duration_s = round(time() - time_start, digits=2)
    time_duration_m = round(time_duration_s/60, digits=2)
    println(
        "Finished decomposing fund returns in $time_duration_s seconds " *
        "($time_duration_m minutes)"
    )
end

extract_betas(regression_outputs) = unstack(
    regression_outputs, [:fundid, :date], :factor, :coef, renamecols=f->Symbol("$(f)_beta")
)

function decompose_returns(data, model)
    factors =  BENCHMARK_MODELS[model[1]] ∪ CURRENCYRISK_MODELS[model[2]]
    decomposed_returns = data[!, [:fundid, :date, :ret]]
    decomposed_returns.ret_alpha .= 0.0

    for factor in factors
        decomposed_returns[!, "ret_$(factor)"] = (
            data[!, factor] .* data[!, "$(factor)_beta"]
        )
    end

    decomposed_returns.ret_alpha = (
        data.ret - sum(decomposed_returns[:, Symbol("ret_$(factor)")] for factor in factors)
    )
    return decomposed_returns
end

function save_decomposition(decomposed_returns, output_folderstring, model_name, savelock)
    lock(savelock) do
        output_filestring = makepath(output_folderstring, "$model_name.arrow")
        Arrow.write(output_filestring, decomposed_returns)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end