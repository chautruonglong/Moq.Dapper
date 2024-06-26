﻿using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Moq.Language.Flow;
using Moq.Protected;

namespace Moq.Dapper
{
    public static class DbConnectionMockExtensions
    {
        static DbConnectionMockExtensions()
        {
            SqlMapper.AddTypeMap(typeof(BigInteger), DbType.String);
        }
        
        public static ISetup<DbConnection, Task<TResult>> SetupDapperAsync<TResult>(this Mock<DbConnection> mock, Expression<Func<DbConnection, Task<TResult>>> expression)
        {
            var call = expression.Body as MethodCallExpression;

            if (call?.Method.DeclaringType != typeof(SqlMapper))
                throw new ArgumentException("Not a Dapper method.");

            switch (call.Method.Name)
            {
                case nameof(SqlMapper.QueryAsync):
                case nameof(SqlMapper.QueryFirstAsync):
                case nameof(SqlMapper.QueryFirstOrDefaultAsync):
                case nameof(SqlMapper.QuerySingleAsync):
                case nameof(SqlMapper.QuerySingleOrDefaultAsync):
                    return SetupQueryAsync<TResult>(mock);
                case nameof(SqlMapper.ExecuteAsync) when typeof(TResult) == typeof(int):
                    return (ISetup<DbConnection, Task<TResult>>)SetupExecuteAsync(mock);
                case nameof(SqlMapper.ExecuteScalarAsync):
                    return SetupExecuteScalarAsync<TResult>(mock);
                default:
                    throw new NotSupportedException();
            }
        }

        static ISetup<DbConnection, Task<TResult>> SetupQueryAsync<TResult>(Mock<DbConnection> mock) =>
            DbCommandSetup.SetupCommandAsync<TResult, DbConnection>(mock, (commandMock, result) =>
            {
                commandMock.Protected()
                           .Setup<Task<DbDataReader>>("ExecuteDbDataReaderAsync", ItExpr.IsAny<CommandBehavior>(), ItExpr.IsAny<CancellationToken>())
                           .ReturnsAsync(() => result().ToDataTable(typeof(TResult))
                                                                  .ToDataTableReader());
            });

        static ISetup<DbConnection, Task<int>> SetupExecuteAsync(Mock<DbConnection> mock) =>
            SetupNonQueryCommandAsync(mock, (commandMock, result) =>
            {
                commandMock.Setup(x => x.ExecuteNonQueryAsync(It.IsAny<CancellationToken>())).ReturnsAsync(result);
            });

        static ISetup<DbConnection, Task<int>> SetupNonQueryCommandAsync(Mock<DbConnection> mock, Action<Mock<DbCommand>, Func<int>> mockResult)
        {
            var setupMock = new Mock<ISetup<DbConnection, Task<int>>>();

            var result = default(int);

            setupMock.Setup(setup => setup.Returns(It.IsAny<Func<Task<int>>>()))
                     .Callback<Func<Task<int>>>(r => result = r().Result);

            var commandMock = new Mock<DbCommand>();

            commandMock.SetupAllProperties();

            var parametersMock = new Mock<DbParameterCollection>();
            parametersMock.Setup(p => p.GetEnumerator())
                          .Returns(new Mock<IEnumerator>().Object);
            commandMock.Protected()
                       .SetupGet<DbParameterCollection>("DbParameterCollection")
                       .Returns(parametersMock.Object);

            commandMock.Protected()
                       .Setup<DbParameter>("CreateDbParameter")
                       .Returns(new Mock<DbParameter>().Object);
            
            mockResult(commandMock, () => result);

            mock.As<IDbConnection>()
                .Setup(m => m.CreateCommand())
                .Returns(commandMock.Object);

            return setupMock.Object;
        }

        static ISetup<DbConnection, Task<TResult>> SetupExecuteScalarCommandAsync<TResult>(Mock<DbConnection> mock, Action<Mock<DbCommand>, Func<object>> mockResult)
        {
            var setupMock = new Mock<ISetup<DbConnection, Task<TResult>>>();

            var result = default(TResult);

            setupMock.Setup(setup => setup.Returns(It.IsAny<Func<Task<TResult>>>()))
                     .Callback<Func<Task<TResult>>>(r => result = r().Result);

            var commandMock = new Mock<DbCommand>();

            var parametersMock = new Mock<DbParameterCollection>();
            parametersMock.Setup(p => p.GetEnumerator())
                          .Returns(new Mock<IEnumerator>().Object);
            commandMock.Protected()
                       .SetupGet<DbParameterCollection>("DbParameterCollection")
                       .Returns(parametersMock.Object);

            commandMock.Protected()
                       .Setup<DbParameter>("CreateDbParameter")
                       .Returns(new Mock<DbParameter>().Object);

            mockResult(commandMock, () => result);

            mock.As<IDbConnection>()
                .Setup(m => m.CreateCommand())
                .Returns(commandMock.Object);

            return setupMock.Object;
        }

        static ISetup<DbConnection, Task<TResult>> SetupExecuteScalarAsync<TResult>(Mock<DbConnection> mock) =>
            SetupExecuteScalarCommandAsync<TResult>(mock, (commandMock, result) =>
            {
                commandMock.Setup(x => x.ExecuteScalarAsync(It.IsAny<CancellationToken>())).ReturnsAsync(result);
            });
    }
}
