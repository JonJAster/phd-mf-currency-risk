using Revise

includet("pipeline/mutual-fund-data/InitMFData.jl")

using .InitMFData

function main()
    init_mf_data()
end