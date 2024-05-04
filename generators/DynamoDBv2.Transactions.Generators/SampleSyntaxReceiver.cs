using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace DynamoDBv2.Transactions.Generators
{
    public sealed class SampleSyntaxReceiver : ISyntaxReceiver
    {
        public List<TypeDeclarationSyntax> MessageTypes { get; } = new();

        public void OnVisitSyntaxNode(SyntaxNode syntaxNode)
        {
#if DEBUG_GENERATOR
	        while (!System.Diagnostics.Debugger.IsAttached)
            Thread.Sleep(500);
#endif


            if (syntaxNode is TypeDeclarationSyntax typeDeclarationSyntax)
            {
                foreach (var attributeList in typeDeclarationSyntax.AttributeLists)
                {
                    foreach (var attribute in attributeList.Attributes)
                    {
                        var name = attribute.Name.ToString();


#if DEBUG_GENERATOR
	        while (!System.Diagnostics.Debugger.IsAttached)
            Thread.Sleep(500);
#endif

                        switch (name)
                        {
                            case "DynamoDBTable":
                                MessageTypes.Add(typeDeclarationSyntax);
                                break;
                        }
                    }
                }
            }
        }
    }
}
