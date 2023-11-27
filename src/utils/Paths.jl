const PATHS = (
    rawfunds = "data/mutual-funds/raw",
    groupedfunds = "data/mutual-funds/domicile-grouped",
    fxfactors = "data/currencies/factor-series/currency_factors.arrow"
)

const _PATH_KEYWORD = r"(?<=\$)[a-zA-Z_]+"

function qpath(paths::AbstractString...; kwargs...)
    path = joinpath(paths...) |> _alignslashes

    # Running in a notebook requires a relative path.
    _isnotebook() && (path = "../../$path")

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

    output_path = joinpath(path_parts) |> _alignslashes

    return output_path
end

"""
    qsave(paths...; data, kwargs...)

Save data to a file. The path can be specified as a string or as a series of
strings and keywords. If the path contains a variable value, the variable
should be specified as a keyword argument.

# Examples
```julia
qsave("data/raw/info/mf_info_other.arrow"; data=info)
qsave("data/raw/info/\\\$file"; data=info, file="mf_info_other.arrow")
qsave(PATHS.mainfunds; data=info, options_folder="local-rets_eq-strict")
```

# Arguments
- `paths...`: The path to the file to be saved. If the path
    contains a variable value, the variable should be specified as a keyword
    argument.
- `data`: The data to be saved.
- `kwargs...`: Keyword arguments. If the path contains a variable value, the
    variable should be specified as a keyword argument.

# Returns
`nothing`
"""
function qsave(paths...; data, kwargs...)
    path = qpath(paths..., kwargs...)
    path_parent = dirname(path)
    
    if !isdir(path_parent)
        mkpath(path_parent)
        println("Missing directory created: $path_parent")
    end

    _savefile(path, data)

    return nothing
end

function qload(paths...; kwargs...)
    qpath_keywords = [m.match for m in eachmatch(_PATH_KEYWORD, reduce(*, paths))]
    path_kwargs = filter(kwarg->kwarg.first ∈ qpath_keywords, kwargs)
    load_kwargs = filter(kwarg->kwarg.first ∉ qpath_keywords, kwargs)

    path = qpath(paths...; path_kwargs...)

    if isdir(path)
        data = _loaddir(path; load_kwargs...)
    elseif isfile(path)
        data = _loadfile(path; load_kwargs...)
    else
        error("Path not found: $path")
    end


    return data
end

function qnames(paths...; printout=false, kwargs...)
    path = qpath(paths...; kwargs...)

    if isdir(path)
        filepath = readdir(path, join=true)
    elseif isfile(path)
        filepath = path
    else
        error("Path not found: $path")
    end

    filetype = _filetype(filepath)

    if filetype == :csv
        colnames = CSV.File(filepath, limit=1) |> propertynames
    elseif filetype == :arrow
        colnames = Arrow.Table(filepath) |> propertynames
    else
        error("Internal error: filetype assertion failed")
    end

    if printout
        for name in colnames
            strname = string(name) |> escape_string
            println(strname)
        end
    end
    return colnames
end

function _isnotebook()
    caller = filter(x -> x.func == Symbol("top-level scope"), stacktrace())
    isempty(caller) ? (return false) : (caller = first(caller))

    caller_is_notebook = !isnothing(match(r"\.ipynb$", string(caller.file)))
    return caller_is_notebook
end

_alignslashes(path) = replace(path, "\\" => "/")