using DataFrames
using CSV
using Arrow
using Dates
using StatsBase

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

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

    datafile = joinpath(DIRS.mf.init, "mf-data.arrow")
    infofile = joinpath(DIRS.mf.init, "mf-info.arrow")
    qhead(joinpath(DIRS.mf.init, "mf-data.arrow"))
    qview(joinpath(DIRS.mf.init, "mf-data.arrow"))

    data = Arrow.Table(datafile)
    data[2]
    x = first(eachcol(DataFrame(data)))
end