using FromFile
using Test
using BenchmarkTools
using DataFrames
using CSV

@from "../src/utils.jl" using ProjectUtilities
@from "../src/pipeline/processing-funds/group-raw-mf-data-by-country.jl" import
    EXPLICIT_TYPES,
    ID_COLS,
    _normalise_headings!,
    _drop_incompleteids!,
    _drop_emptycols!,
    _drop_emptyrows!

function test_droplostdata()
    @testset "test_droplostdata" begin
    input = DataFrame(
        name = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        fundid = ["a", "b", "c", missing, "e", "f", "g", "h", "i", "j"],
        secid = ["a", "b", "c", "d", "e", "f", "g", "h", missing, missing],
        data1 = [missing, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        data2 = [missing, missing, missing, missing, missing, missing, missing, missing, missing, missing],
        data3 = [missing, missing, 3, 4, 5, 6, 7, 8, 9, 10]
    )

    df = copy(input)
    _drop_incompleteids!(df)
    expected = DataFrame(
        name = ["a", "b", "c", "e", "f", "g", "h"],
        fundid = ["a", "b", "c", "e", "f", "g", "h"],
        secid = ["a", "b", "c", "e", "f", "g", "h"],
        data1 = [missing, 2, 3, 5, 6, 7, 8],
        data2 = [missing, missing, missing, missing, missing, missing, missing],
        data3 = [missing, missing, 3, 5, 6, 7, 8]
    )

    @test isequal(df, expected)

    df = copy(input)
    _drop_emptycols!(df)
    expected = DataFrame(
        name = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        fundid = ["a", "b", "c", missing, "e", "f", "g", "h", "i", "j"],
        secid = ["a", "b", "c", "d", "e", "f", "g", "h", missing, missing],
        data1 = [missing, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        data3 = [missing, missing, 3, 4, 5, 6, 7, 8, 9, 10]
    )

    @test isequal(df, expected)

    df = copy(input)
    _drop_emptyrows!(df)
    expected = DataFrame(
        name = ["b", "c", "d", "e", "f", "g", "h", "i", "j"],
        fundid = ["b", "c", missing, "e", "f", "g", "h", "i", "j"],
        secid = ["b", "c", "d", "e", "f", "g", "h", missing, missing],
        data1 = [2, 3, 4, 5, 6, 7, 8, 9, 10],
        data2 = [missing, missing, missing, missing, missing, missing, missing, missing, missing],
        data3 = [missing, 3, 4, 5, 6, 7, 8, 9, 10]
    )

    @test isequal(df, expected)
    end # @testset "test_droplostdata"
end

function speedtest_droplostdata()
    println("time load of data") #406 seconds (6.8 minutes)
    startedat = time()
    data = qload(
        PATHS.rawfunds, "monthly-net-assets";
        groupmark=',', types=EXPLICIT_TYPES, stringtype=String
    )
    printtime("loading data", startedat)

    _normalise_headings!(data)

    println("benchmark _drop_incompleteids!")
    display(@benchmark _drop_incompleteids!(df) setup=(df=copy(data)) evals=1)

    println("benchmark _drop_emptycols!")
    display(@benchmark _drop_emptycols!(df) setup=(df=copy(data)) evals=1)

    println("benchmark _drop_emptyrows!")
    display(@benchmark _drop_emptyrows!(df) setup=(df=copy(data)) evals=1)
end

if abspath(PROGRAM_FILE) == @__FILE__
    test_droplostdata();
    speedtest_droplostdata()
end
