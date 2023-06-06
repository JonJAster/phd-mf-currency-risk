dropifany!(df, within; inds=nothing, dims=:col) = _dropif!(df, within, inds, dims, any)
dropifall!(df, within; inds=nothing, dims=:col) = _dropif!(df, within, inds, dims, all)

function _dropif!(df, within, inds, dims, combine)
    dims ∉ [:row, :col, 1, 2, :both] && error("'dims' must be :row, :col or :both")
    isequal(within, nothing) || isequal(within, []) && return df
    isempty(df) && return df

    iswithin = (
        if ismissing(within) || typeof(within) <: AbstractVector{Missing}
            x -> ismissing(x)
        elseif typeof(within) <: AbstractVector
            missingtreatment = any(ismissing, within)
            filter!(!ismissing, within)
            x -> coalesce(x ∈ within, missingtreatment)
        else
            x -> x === within
        end
    )

    if dims in [:col, 1, :both]
        isnothing(inds) && (inds = 1:ncol(df))

        selected_cols = df[!, inds]
        typeof(selected_cols) <: AbstractVector && (selected_cols = df[!, [inds]])

        drop_condition = combine.(iswithin, eachcol(selected_cols))
        dropped_cols = names(selected_cols)[drop_condition]
        
        # If 'any', then we can't drop the columns yet, but if 'all' then dropping now
        # will save computation if ... 
        combine == all && select!(df, Not(dropped_cols))
    end

    if dims in [:row, 2, :both]
        isnothing(inds) && (inds = 1:nrow(df))
        inds isa InvertedIndex && (inds = (1:nrow(df))[inds])
        typeof(inds) <: AbstractVector || (inds = [inds])

        drop_condition = combine.(iswithin, eachrow(df[inds,:]))
        dropped_rows = inds[drop_condition]
        delete!(df, dropped_rows)
    end

    combine == any && select!(df, Not(dropped_cols))

    return df
end