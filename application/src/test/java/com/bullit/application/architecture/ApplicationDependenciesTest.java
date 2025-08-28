package com.bullit.application.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(
        packages = "com.bullit",
        importOptions = { ImportOption.DoNotIncludeTests.class }
)
public class ApplicationDependenciesTest {

    @ArchTest
    static final ArchRule only_application_has_bootstrap =
            classes().that().areAnnotatedWith(org.springframework.boot.autoconfigure.SpringBootApplication.class)
                    .should().resideInAnyPackage("com.bullit.application..");

    @ArchTest
    static final ArchRule non_app_modules_should_not_depend_on_autoconfig =
            noClasses().that().resideOutsideOfPackage("com.bullit.application..")
                    .should().dependOnClassesThat().resideInAnyPackage("org.springframework.boot.autoconfigure..");
}