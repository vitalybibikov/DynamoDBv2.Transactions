#define DEBUG_GENERATOR
using DynamoDBv2.Transactions.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Collections.Generic;
using System.Linq;

[Generator]
public class DynamoDbHashKeyValidationGenerator : ISourceGenerator
{
    public void Initialize(GeneratorInitializationContext context)
    {
#if DEBUG
        System.Diagnostics.Debugger.Launch();
#endif

        context.RegisterForSyntaxNotifications(() => new SampleSyntaxReceiver());
    }

    public void Execute(GeneratorExecutionContext context)
    {
        #if DEBUG_GENERATOR
	        while (!System.Diagnostics.Debugger.IsAttached)
            Thread.Sleep(500);
        #endif

        if (context.SyntaxReceiver is SampleSyntaxReceiver receiver)
        {
            var messageTypes = string.Join(" ", receiver.MessageTypes.Select(t => t.TryGetInferredMemberName()));

            context.AddSource("Example.g.cs",
                $@"public static class Example {{
                              public const string Messages = ""{messageTypes}"";
                          }}");
        }
    }

}
