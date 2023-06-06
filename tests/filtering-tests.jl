using FromFile
using DataFrames
using Test

@from "../src/utils.jl" using ProjectUtilities

const m = missing

const TEST_INPUTS = (
    no_missing = DataFrame(
        a=[1, 2, 3, 4, 5],
        b=[2, 4, 6, 8, 0],
        c=[5, 4, 3, 2, 1]
    ),
    random_missing = DataFrame(
        a=[1, 2, m, 4, 5],
        b=[m, 4, 6, 8, 0],
        c=[5, 4, 3, m, 1]
    ),
    a_missing = DataFrame(
        a=[m, m, m, m, m],
        b=[2, 4, 6, 8, 0],
        c=[5, 4, 3, 2, 1]
    ),
    row13_missing = DataFrame(
        a=[m, 2, m, 4, 5],
        b=[m, 4, m, 8, 0],
        c=[m, 4, m, 2, 1]
    ),
    empty_strings = DataFrame(
        a=["", "a", "b", "c", "d"],
        b=["", "e", "f", "g", "h"],
        c=["", "i", "j", "k", "l"]
    ),
    empty_and_missing = DataFrame(
        a=["", m, m, m, m],
        b=["", "e", "f", "g", "h"],
        c=["", "i", "j", "k", "l"]
    )
)

const ROWLESS_OUTPUT = filter(x->false, TEST_INPUTS.no_missing)

function test_dropifany!()
    @testset "dropifany!" begin
        expected = DataFrame(
            a=[1, 2, 3, 4, 5],
            b=[2, 4, 6, 8, 0],
            c=[5, 4, 3, 2, 1]
        )
        input = copy(TEST_INPUTS.no_missing)
        dropifany!(input, missing)
        @test isequal(input, expected)

        expected = DataFrame()
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing)
        @test isequal(input, expected)

        expected = DataFrame(
            b=[2, 4, 6, 8, 0],
            c=[5, 4, 3, 2, 1]
        )
        input = copy(TEST_INPUTS.a_missing)
        dropifany!(input, missing)
        @test isequal(input, expected)

        expected = DataFrame(
            b=["", "e", "f", "g", "h"],
            c=["", "i", "j", "k", "l"]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifany!(input, missing)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[2, 5],
            b=[4, 0],
            c=[4, 1]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; dims=:row)
        @test isequal(input, expected)

        expected = ROWLESS_OUTPUT
        input = copy(TEST_INPUTS.a_missing)
        dropifany!(input, missing; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[2, 4, 5],
            b=[4, 8, 0],
            c=[4, 2, 1]
        )
        input = copy(TEST_INPUTS.row13_missing)
        dropifany!(input, missing; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[""],
            b=[""],
            c=[""]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifany!(input, missing; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            c=[5, 4, 3, m, 1]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; inds=1:2, dims=:col)
        @test isequal(input, expected)

        expected = DataFrame(
            c=[5, 4, 3, m, 1]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; inds=[:a, :b])
        @test isequal(input, expected)

        expected = DataFrame(
            a=[1, 2, m, 4, 5],
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; inds=Not(:a))
        @test isequal(input, expected)

        expected = DataFrame(
            a=[1, 2, m, 4, 5],
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; inds=Between(:b, :c))
        @test isequal(input, expected)

        expected = DataFrame(
            a=[2, 5],
            b=[4, 0],
            c=[4, 1]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; inds=1:5, dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[2, 4, 5],
            b=[4, 8, 0],
            c=[4, m, 1]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; inds=1:3, dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[1, 2, m, 5],
            b=[m, 4, 6, 0],
            c=[5, 4, 3, 1]
        )
        input = copy(TEST_INPUTS.random_missing)
        @test begin
            dropifany!(input, missing; inds=Not(1:3), dims=:row)
            isequal(input, expected)
        end

        expected = copy(TEST_INPUTS.random_missing)
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, [])
        @test isequal(input, expected)

        expected = DataFrame()
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, [missing])
        @test isequal(input, expected)

        expected = DataFrame(
            b=[2, 4, 6, 8, 0]
        )
        input = copy(TEST_INPUTS.no_missing)
        dropifany!(input, [missing, 3])
        @test isequal(input, expected)

        expected = DataFrame(
            a=[5],
            b=[0],
            c=[1]
        )
        input = copy(TEST_INPUTS.row13_missing)
        dropifany!(input, [missing, 2]; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[3, 5],
            b=[6, 0],
            c=[3, 1]
        )
        input = copy(TEST_INPUTS.no_missing)
        dropifany!(input, [2, 4]; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame()
        input = copy(TEST_INPUTS.empty_strings)
        dropifany!(input, "")
        @test isequal(input, expected)

        expected = DataFrame(
            a=["a", "b", "c", "d"],
            b=["e", "f", "g", "h"],
            c=["i", "j", "k", "l"]
        )
        input = copy(TEST_INPUTS.empty_strings)
        dropifany!(input, ""; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[m, m, m, m],
            b=["e", "f", "g", "h"],
            c=["i", "j", "k", "l"]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifany!(input, "", dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[m, m],
            b=["g", "h"],
            c=["k", "l"]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifany!(input, ["", missing]; inds=1:3, dims=:row)
        @test isequal(input, expected)

        expected = DataFrame()
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; dims=:both)
        @test isequal(input, expected)

        expected = DataFrame(
            b=[4, 6, 8, 0],
            c=[4, 3, m, 1]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifany!(input, missing; inds=1, dims=:both)
        @test isequal(input, expected)

        expected = DataFrame(
            b=[""],
            c=[""]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifany!(input, missing; dims=:both)
        @test isequal(input, expected)

        expected = DataFrame(
            b=[4, 6, 8]
        )
        input = copy(TEST_INPUTS.no_missing)
        dropifany!(input, 1; dims=:both)
        @test isequal(input, expected)
    end
end

function test_dropifall!()
    @testset "dropifall!" begin
        expected = DataFrame(
            a=[1, 2, m, 4, 5],
            b=[m, 4, 6, 0, 8],
            c=[5, 4, 3, 1, m]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifall!(input, missing; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[1, 2, m, 4, 5],
            b=[m, 4, 6, 0, 8],
            c=[5, 4, 3, 1, m]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifall!(input, missing; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            b=[2, 4, 6, 8, 0],
            c=[5, 4, 3, 2, 1]
        )
        input = copy(TEST_INPUTS.a_missing)
        dropifall!(input, missing)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[m, m, m, m, m],
            b=[2, 4, 6, 8, 0],
            c=[5, 4, 3, 2, 1]
        )
        input = copy(TEST_INPUTS.a_missing)
        dropifall!(input, missing; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=[2, 4, 5],
            b=[4, 8, 0],
            c=[4, 2, 1]
        )
        input = copy(TEST_INPUTS.row13_missing)
        dropifall!(input, missing; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            a=["", m, m, m, m],
            b=["", "e", "f", "g", "h"],
            c=["", "i", "j", "k", "l"]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifall!(input, missing)
        @test isequal(input, expected)

        expected = DataFrame(
            a=["", m, m, m, m],
            b=["", "e", "f", "g", "h"],
            c=["", "i", "j", "k", "l"]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifall!(input, [missing, ""])
        @test isequal(input, expected)

        expected = DataFrame(
            a=[m, m, m, m],
            b=["e", "f", "g", "h"],
            c=["i", "j", "k", "l"]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifall!(input, ""; dims=:row)
        @test isequal(input, expected)

        expected = DataFrame(
            b=["", "e", "f", "g", "h"],
            c=["", "i", "j", "k", "l"]
        )
        input = copy(TEST_INPUTS.empty_and_missing)
        dropifall!(input, [missing, ""])
        @test isequal(input, expected)

        expected = DataFrame(
            b=[m, 4, 6, 8, 0],
            c=[5, 4, 3, m, 1]
        )
        input = copy(TEST_INPUTS.random_missing)
        dropifall!(input, [1,2,4,5,missing]; dims=:row)
        @test isequal(input, expected)
    end
end

test_dropifany!();
test_dropifall!();