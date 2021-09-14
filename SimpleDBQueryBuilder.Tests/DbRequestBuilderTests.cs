using GHSoftware.SimpleDb;
using Xunit;

namespace SimpleDBQueryBuilder.Tests;
public class DbRequestBuilderTests
{

    enum DbFiels
    {
        id,
        name,
        date,
        value
    }

    [Fact]
    public void SelectSimpleTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithSelectFields(DbFiels.id, DbFiels.name, DbFiels.value)
            .BuildSelect();


        Assert.Equal("select id,name,value from thing", req.Sql);
        Assert.Equal(DbRequest.CmdType.Query, req.Type);

    }

    [Fact]
    public void SelectWithWhereTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithSelectFields(DbFiels.name, DbFiels.value)
            .WhereCond(DbFiels.id, 54)
            .BuildSelect();


        Assert.Equal("select name,value from thing where id = @id ", req.Sql);
        Assert.Equal(54, req.Parameters[0].Value);
        Assert.Equal(DbRequest.CmdType.Query, req.Type);

    }


    [Fact]
    public void SelectWithWhereAndScalarTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithSelectFields(DbFiels.name, DbFiels.value)
            .WhereCond(DbFiels.id, 54)
            .SetScalar(true)
            .BuildSelect();


        Assert.Equal("select name,value from thing where id = @id ", req.Sql);
        Assert.Equal(54, req.Parameters[0].Value);
        Assert.Equal(DbRequest.CmdType.SingleResult, req.Type);
    }

    [Fact]
    public void SelectWithWhereAndOrderTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithSelectFields(DbFiels.name, DbFiels.value)
            .WhereCond(DbFiels.date, new DateTime(2000, 01, 01), ">")
            .SetOrder($"{DbFiels.name} desc")
            .BuildSelect();


        Assert.Equal("select name,value from thing where date > @date  order by name desc", req.Sql);
        Assert.Equal(new DateTime(2000, 01, 01), req.Parameters[0].Value);
        Assert.Equal(DbRequest.CmdType.Query, req.Type);

    }


    [Fact]
    public void SelectWithWhereAndOrderAndFieldCharTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithFieldQuotes()
            .WithSelectFields(DbFiels.name, DbFiels.value)
            .WhereCond(DbFiels.date, new DateTime(2000, 01, 01), ">")
            .SetOrder($"\"{DbFiels.name}\" desc")
            .BuildSelect();


        Assert.Equal(@"select ""name"",""value"" from ""thing"" where ""date"" > @date  order by ""name"" desc", req.Sql);
        Assert.Equal(new DateTime(2000, 01, 01), req.Parameters[0].Value);
        Assert.Equal(DbRequest.CmdType.Query, req.Type);

    }

    [Fact]
    public void SelectWithoutFieldsTest()
    {


        Assert.Throws<ArgumentException>(() =>
      {
          var req =
          DbRequestBuilder.Of<DbFiels>()
              .WithTable("thing")
              .WhereCond(DbFiels.id, 54)
              .BuildSelect();
      });
    }


    [Fact]
    public void InsertWithFieldCharTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithFieldQuotes()
            .WithFieldValues(DbFiels.name, "thing name'")
            .WithFieldValues(DbFiels.value, 15.78)
            .WithFieldValues(DbFiels.date, new DateTime(2000, 01, 01))
            .BuildInsert();


        Assert.Equal(@"insert into ""thing"" (""name"",""value"",""date"") values (@name,@value,@date) ", req.Sql);
        Assert.Equal("thing name'", req.Parameters[0].Value);
        Assert.Equal(15.78, req.Parameters[1].Value);
        Assert.Equal(new DateTime(2000, 01, 01), req.Parameters[2].Value);
        Assert.Equal(DbRequest.CmdType.CommandWithIdentity, req.Type);
    }

    [Fact]
    public void InsertWithScalarTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithFieldValues(DbFiels.name, "thing name'")
            .WithFieldValues(DbFiels.value, 15.78)
            .WithFieldValues(DbFiels.date, new DateTime(2000, 01, 01))
            .SetScalar(true)
            .BuildInsert();


        Assert.Equal(@"insert into thing (name,value,date) values (@name,@value,@date) ", req.Sql);
        Assert.Equal("thing name'", req.Parameters[0].Value);
        Assert.Equal(15.78, req.Parameters[1].Value);
        Assert.Equal(new DateTime(2000, 01, 01), req.Parameters[2].Value);
        Assert.Equal(DbRequest.CmdType.CommandWithIdentity, req.Type);
    }

    [Fact]
    public void InsertWithFieldCharAndAppendTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithFieldQuotes()
            .WithFieldValues(DbFiels.name, "thing name'")
            .WithFieldValues(DbFiels.value, 15.78)
            .WithFieldValues(DbFiels.date, new DateTime(2000, 01, 01))
            .AppendToQuery(" anything else here ")
            .BuildInsert();


        Assert.Equal(@"insert into ""thing"" (""name"",""value"",""date"") values (@name,@value,@date)  anything else here ", req.Sql);
        Assert.Equal("thing name'", req.Parameters[0].Value);
        Assert.Equal(15.78, req.Parameters[1].Value);
        Assert.Equal(new DateTime(2000, 01, 01), req.Parameters[2].Value);
        Assert.Equal(DbRequest.CmdType.CommandWithIdentity, req.Type);

    }

    [Fact]
    public void UpdateWithWhereTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithFieldValues(DbFiels.name, "thing name'")
            .WithFieldValues(DbFiels.value, 15.78)
            .WithFieldValues(DbFiels.date, new DateTime(2000, 01, 01))
            .WhereCond(DbFiels.id, 20)
            .BuildUpdate();


        Assert.Equal(@"update thing set name=@name,value=@value,date=@date where id = @id ", req.Sql);
        Assert.Equal("thing name'", req.Parameters[0].Value);
        Assert.Equal(15.78, req.Parameters[1].Value);
        Assert.Equal(new DateTime(2000, 01, 01), req.Parameters[2].Value);
        Assert.Equal(20, req.Parameters[3].Value);
        Assert.Equal(DbRequest.CmdType.Command, req.Type);

    }


    [Fact]
    public void UpdateWithWhereAndAppendTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithFieldValues(DbFiels.name, "thing name'")
            .WithFieldValues(DbFiels.value, 15.78)
            .WithFieldValues(DbFiels.date, new DateTime(2000, 01, 01))
            .WhereCond(DbFiels.id, 20)
            .AppendToQuery("custom")
            .BuildUpdate();


        Assert.Equal(@"update thing set name=@name,value=@value,date=@date where id = @id custom", req.Sql);
        Assert.Equal("thing name'", req.Parameters[0].Value);
        Assert.Equal(15.78, req.Parameters[1].Value);
        Assert.Equal(new DateTime(2000, 01, 01), req.Parameters[2].Value);
        Assert.Equal(20, req.Parameters[3].Value);
        Assert.Equal(DbRequest.CmdType.Command, req.Type);

    }



    [Fact]
    public void UpdateWithFieldArrayWhereTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WithFieldValues(new List<KeyValuePair<string, object>> {
                new KeyValuePair<string, object>("name", "thing name'"),
                new KeyValuePair<string, object>("value", 15.78),
                new KeyValuePair<string, object>("date", new DateTime(2000, 01, 01)),
            })
            .WhereCond(DbFiels.id, 20)
            .BuildUpdate();

        Assert.Equal(@"update thing set name=@name,value=@value,date=@date where id = @id ", req.Sql);
        Assert.Equal("thing name'", req.Parameters[0].Value);
        Assert.Equal(15.78, req.Parameters[1].Value);
        Assert.Equal(new DateTime(2000, 01, 01), req.Parameters[2].Value);
        Assert.Equal(20, req.Parameters[3].Value);

        Assert.Equal(DbRequest.CmdType.Command, req.Type);
    }

    [Fact]
    public void DeleteWithWhereTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WhereCond(DbFiels.id, 20)
            .BuildDelete();

        Assert.Equal(@"delete from thing  where id = @id ", req.Sql);
        Assert.Equal(20, req.Parameters[0].Value);
        Assert.Equal(DbRequest.CmdType.Command, req.Type);
    }


    [Fact]
    public void DeleteWithWhereAndAppendTest()
    {

        var req =
        DbRequestBuilder.Of<DbFiels>()
            .WithTable("thing")
            .WhereCond(DbFiels.id, 20)
            .AppendToQuery("custom")
            .BuildDelete();

        Assert.Equal(@"delete from thing  where id = @id custom", req.Sql);
        Assert.Equal(20, req.Parameters[0].Value);
        Assert.Equal(DbRequest.CmdType.Command, req.Type);
    }


    [Fact]
    public void DeleteWithoutWhereTest()
    {
        Assert.Throws<ArgumentException>(() =>
        {
            var req =
            DbRequestBuilder.Of<DbFiels>()
                .WithTable("thing")
                .BuildDelete();
        });
    }
}