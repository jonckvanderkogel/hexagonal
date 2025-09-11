package com.bullit.web.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(
        packages = "com.bullit.web",
        importOptions = { ImportOption.DoNotIncludeTests.class }
)
public class WebDependenciesTest {

    @ArchTest
    static final ArchRule web_should_depend_only_on_domain_from_project_code =
            noClasses()
                    .that()
                    .resideInAnyPackage("com.bullit.web..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "com.bullit.core..",
                            "com.bullit.data..",
                            "com.bullit.application.."
                    );

    @ArchTest
    static final ArchRule web_should_not_depend_on_jpa =
            noClasses()
                    .that()
                    .resideInAnyPackage("com.bullit.web..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "jakarta.persistence..",
                            "javax.persistence..",
                            "org.hibernate..",
                            "org.springframework.data.."
                    );

    @ArchTest
    static final ArchRule web_should_not_depend_on_spring_boot =
            noClasses().that().resideInAnyPackage("com.bullit.web..")
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "org.springframework.boot..",           // all Boot APIs
                            "org.springframework.boot.autoconfigure..",
                            "org.springframework.boot.context.."
                    );
}