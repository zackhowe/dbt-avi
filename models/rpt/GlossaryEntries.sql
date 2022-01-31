SELECT GlossaryTerm, GlossaryDefinition
FROM {{ source('rpt', 'GlossaryEntries') }}