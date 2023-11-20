using DataFrames
using Arrow

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const INPUT_DIR_MF = joinpath(DIRS.fund, "post-processing")

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    for model in COMPLETE_MODELS
        model_name = name_model(model)
        filename = joinpath(INPUT_DIR_MF, options_folder, "flow-betas/$model_name.arrow")
        flow_betas = Arrow.Table(filename) |> DataFrame
        display(flow_betas)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
