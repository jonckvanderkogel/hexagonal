package com.bullit.data.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(
        packages = "com.bullit.data",
        importOptions = { ImportOption.DoNotIncludeTests.class }
)
public class DataDependenciesTest {

    @ArchTest
    static final ArchRule data_should_depend_only_on_domain_from_project_code =
            noClasses().that().resideInAnyPackage("com.bullit.data..")
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "com.bullit.core..",
                            "com.bullit.web..",
                            "com.bullit.application.."
                    );
}