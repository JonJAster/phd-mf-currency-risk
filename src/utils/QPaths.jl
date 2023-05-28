const PATHS = (
    rawfunds = "data/mutual-funds/raw"
)

function qpath(path; notebook=false, kwargs...)
    notebook && (path = "../../$path")
    '$' ∉ path && return path
    
    path_parts = split(path, "/")
    for i in eachindex(path_parts)
        part = path_parts[i]
        part[1] != '$' && continue

        arg = Symbol(part[2:end])
        value = get(kwargs, arg, nothing)
        isnothing(value) && error("Expected $arg to be supplied")

        path_parts[i] = value
    end

    output_path = joinpath(path_parts)
    replace!(output_path, r"\\\\"=>r"/")

    return output_path
end