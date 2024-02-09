using CSV
using DataFrames

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/mutual-fund-data/init-mf-data.jl")

using .CommonConstants
using .CommonFunctions

function test()
    folder = "data/mutual-funds/raw"
    files = readdir(folder)
    data = Dict()
    for file in files
        data[file] = CSV.read(joinpath(folder, file), DataFrame)
    end

    k = collect(keys(data))

    for i in k
        testdf = data[i]
        n_nonmissing = sum(.!ismissing.(testdf[:, 4]))
        println(i, ": ", n_nonmissing)
    end

    display(first(first(data)[2]))
    CommonFunctions._normalise_names!(first(data)[2])

    dftest = init_mf_data()

    dftest

    infodata = CSV.read(joinpath(DIRS.mf.raw, "info.csv"), DataFrame)
end