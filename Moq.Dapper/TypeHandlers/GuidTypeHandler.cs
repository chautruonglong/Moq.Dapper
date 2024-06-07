using System;
using System.Data;
using Dapper;

namespace Moq.Dapper.TypeHandlers;

public class GuidTypeHandler : SqlMapper.TypeHandler<Guid>
{
    public override void SetValue(IDbDataParameter parameter, Guid guid)
    {
        parameter.Value = guid.ToString();
    }

    public override Guid Parse(object value)
    {
        return new Guid((string)value);
    }
}