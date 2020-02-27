

def consistent_groupby(df, labels, function):
        # no tags then no need to group values
        if len(labels) == 0:
            function(df, "the measurement")
            return
        # only one tag then convert str to list the indices
        elif len(labels) == 1:
            for indices, data in df.groupby(labels):
                function(data, dict(zip(labels, [indices])))
        # more tags group by every combination
        else:
            for indices, data in df.groupby(labels):
                function(data, dict(zip(labels, indices)))