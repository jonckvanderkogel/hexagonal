package com.bullit.core.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

@AnalyzeClasses(
        packages = "com.bullit.core",
        importOptions = { ImportOption.DoNotIncludeTests.class }
)
public class CoreDependenciesTest {

    @ArchTest
    static final ArchRule core_should_not_depend_on_spring_or_webmvc =
            noClasses().that().resideInAnyPackage("com.bullit.core..")
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "org.springframework..",
                            "org.springframework.boot..",
                            "org.springframework.web.."
                    );

    @ArchTest
    static final ArchRule core_should_not_depend_on_jpa_or_db_layers =
            noClasses().that().resideInAnyPackage("com.bullit.core..")
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "jakarta.persistence..",
                            "javax.persistence..",
                            "org.hibernate..",
                            "org.springframework.data.."
                    );

    @ArchTest
    static final ArchRule core_should_not_depend_on_driving_or_boot_modules =
            noClasses().that().resideInAnyPackage("com.bullit.core..")
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "com.bullit.web..",
                            "com.bullit.data..",
                            "com.bullit.application.."
                    );

    @ArchTest
    static final ArchRule core_should_depend_only_on_domain_or_itself_for_project_code =
            noClasses().that().resideInAnyPackage("com.bullit.core..")
                    .should().dependOnClassesThat().resideOutsideOfPackages(
                            "com.bullit.domain..",
                            "com.bullit.core..",   // ← allow core to depend on itself
                            "java..",
                            "javax..",
                            "jakarta.."
                    );
}