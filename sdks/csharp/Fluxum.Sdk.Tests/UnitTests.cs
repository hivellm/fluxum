// The SDK's own unit surface: the FluxBIN decode path, pinned directly. The
// envelope/MessagePack codec is internal and is exercised end-to-end by the
// conformance corpus (SPEC-013 — the corpus asserts protocol-observable
// behavior; an SDK's own suite pins how it surfaces the wire).

using System;
using System.Collections.Generic;
using Xunit;
using Fluxum.Sdk;

namespace Fluxum.Sdk.Tests;

public class UnitTests
{
    [Fact]
    public void FluxbinReadsEachTypeLittleEndian()
    {
        var buf = new List<byte>();
        buf.AddRange(BitConverter.GetBytes((ulong)1)); // id: U64
        buf.Add(0x01);                                 // done: Bool
        buf.AddRange(BitConverter.GetBytes(-5));       // x: I32
        buf.AddRange(BitConverter.GetBytes((uint)2));  // Str length
        buf.AddRange(System.Text.Encoding.UTF8.GetBytes("hi"));
        var row = FluxBin.DecodeRow(buf.ToArray(), new[]
        {
            new Column("id", "U64"), new Column("done", "Bool"),
            new Column("x", "I32"), new Column("name", "Str"),
        });
        Assert.Equal((ulong)1, row["id"]);
        Assert.Equal(true, row["done"]);
        Assert.Equal((long)-5, row["x"]);
        Assert.Equal("hi", row["name"]);
    }

    [Fact]
    public void FluxbinIdentityIsHexAndTrailingBytesAreAnError()
    {
        var ident = new byte[32];
        for (int i = 0; i < 32; i++) ident[i] = (byte)i;
        Assert.Equal("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            new RowReader(ident).Read("Identity"));
        Assert.Throws<FluxBinException>(() =>
            FluxBin.DecodeRow(new byte[] { 0x01, 0x02 }, new[] { new Column("b", "Bool") }));
    }
}
