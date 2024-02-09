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
    datafile = joinpath(DIRS.mf.init, "mf-data.arrow")
    infofile = joinpath(DIRS.mf.init, "mf-info.arrow")
    qhead(joinpath(DIRS.mf.init, "mf-data.arrow"))
    qview(joinpath(DIRS.mf.init, "mf-data.arrow"))

    data = Arrow.Table(datafile)
    for col in propertynames(data)
        describe(data[col])
    end
    describe(data.costs)

    df_data = DataFrame(data)

    describe(df_data)

    df_data.date
    data[2]
    x = first(eachcol(DataFrame(data)))
end