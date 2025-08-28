package com.bullit.domain.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(
        packages = "com.bullit.domain",
        importOptions = { ImportOption.DoNotIncludeTests.class }
)
public class DomainDependenciesTest {

    @ArchTest
    static final ArchRule domain_should_not_depend_on_spring =
            noClasses()
                    .that()
                    .resideInAnyPackage("com.bullit.domain..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "org.springframework..",
                            "org.springframework.boot.."
                    );

    @ArchTest
    static final ArchRule domain_should_not_depend_on_jpa_or_web =
            noClasses()
                    .that()
                    .resideInAnyPackage("com.bullit.domain..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "jakarta.persistence..",
                            "javax.persistence..",
                            "org.hibernate..",
                            "org.springframework.web.."
                    );

    @ArchTest
    static final ArchRule domain_should_not_depend_on_other_project_layers =
            noClasses()
                    .that()
                    .resideInAnyPackage("com.bullit.domain..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "com.bullit.core..",
                            "com.bullit.web..",
                            "com.bullit.data..",
                            "com.bullit.application.."
                    );
}
