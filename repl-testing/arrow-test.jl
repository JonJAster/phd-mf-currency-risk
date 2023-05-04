using DataFrames
using Arrow

struct AnnotatedDataFrame
    data::DataFrame
    model::Tuple{String, String}
end

testdf = DataFrame(a = 1:3, b = [missing, ([4,5,6], [7,8,9], 0.1), ([10,11,12], [13,14,15], 0.2)])


Arrow.write("test.arrow", testdf)

testdf2 = Arrow.Table("test.arrow") |> DataFrame