function meantime(x)
    t = Int64[];
    for a in x
        push!(t, a.value)
    end
    return mean(t);
end

function stdtime(x)
    t = Int64[];
    for a in x
        push!(t, a.value)
    end
    return std(t);
end

function earliest(x)
    earliest = x[1];
    for a = 1:length(x) - 1
        x[a + 1] < x[a] && (earliest = x[a + 1]);
    end
    return earliest;
end
