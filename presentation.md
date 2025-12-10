# Hexagonal Architecture: Designing for Change

> “The Universe operates on a basic principle of economics: everything has its cost. We pay to create our future, we pay 
> for the mistakes of the past. We pay for every change we make … and we pay just as dearly if we refuse to change.”

*Brian Herbert & Kevin J. Anderson, Dune: House Harkonnen*

---

## Agenda

Agenda
1.  Costs of change
2.	Hexagonal architecture: core ideas
3.	Hexagonal in practice: rules, modules, boundaries
4.	Mapping to the sample repo
5.	Testing in Hexagonal architecture
6.	When to choose Hexagonal

---

## 1) Costs of change
* Long-lived systems pay a “change tax”: frameworks evolve, infra moves, UIs shift, databases swap.
* Organize code so mistakes are cheap and technology migrations don’t cascade.
* Decouple your core logic from infrastructure so testing becomes easy without (too much) mocking.

**Key principle**: Make the cost of doing the right thing lower than the cost of doing the wrong thing.

---

## 2) Hexagonal architecture: core ideas

> Allow an application to equally be driven by users, programs, automated test or batch scripts, and to be developed and
> tested in isolation from its eventual run-time devices and databases.

*Alistair Cockburn, “Hexagonal architecture“*

- **Origin**: Alistair Cockburn’s “Ports & Adapters”.
- **Center**: Core business logic (domain + use cases) is independent from external dependencies, such as frameworks, 
databases, HTTP/message brokers, or UI. This makes the core logic easier to test and maintain.
- **Ports**: Abstract interfaces describing what the core needs (driven/outbound) or offers (driving/inbound).
  - **Driving**: specifies how the domain can be used.
  - **Driven**: specifies what functionality the domain needs.
- **Adapters**: Technology-specific implementations (HTTP controllers, JPA, messaging, CLI).
  - **Driving**: converts requests from a specific technology to the domain.
  - **Driven**: converts calls from the domain into specific technology terms.
- **Dependency rule**: Edges depend on the center, never the other way around.

---

## 3) Hexagonal in practice: rules, modules, boundaries

**Rules of thumb**
1.	Domain knows business language and invariants; no imports from Spring/JPA/HTTP.
2.	Use cases orchestrate workflows through ports; they depend only on domain + ports.
3.	Adapters translate: HTTP ↔ DTOs ↔ domain; JPA ↔ entities ↔ domain.
4.	Composition root (Spring Boot) wires adapters to ports.
5.	Contracts (ports) encode change boundaries.

**Typical module layout**
* domain: entities/value objects, errors, ports (interfaces).
* core: use case implementations (pure Java).
* web: HTTP adapter (handlers/controllers, DTOs, error mapping).
* data: persistence adapter (JPA entities/repos, mappers).
* application: Spring Boot wiring, routes, config.

---

## 4) Mapping to the sample repo

* Driving adapter: AuthorHttpHandler, RoyaltyHttpHandler (web).
* Driving port: LibraryServicePort, RoyaltyServicePort (domain defines).
* Use cases: LibraryServiceImpl, RoyaltyServiceImpl (core implements).
* Driven ports: AuthorRepositoryPort, BookRepositoryPort, SaleRepositoryPort, SalesReportingPort (domain defines).
* Driven adapters: AuthorRepositoryAdapter, BookRepositoryAdapter, SaleRepositoryAdapter, SalesReportingAdapter (data implements).
* Wiring: BeansConfig (application) connects adapters ↔ ports, installs error filter, routes.

```mermaid
flowchart TB
%% Core (owns ports; use cases implement inbound ports and use outbound ports)
    subgraph Core["Core (Domain + Use Cases)"]
        D["Domain Model"]
        IP["«driving ports»\nLibraryServicePort, RoyaltyServicePort"]
        UC["Use Cases (implement driving ports)\nLibraryServiceImpl, RoyaltyServiceImpl"]
        OP["«driven ports»\nAuthorRepositoryPort, BookRepositoryPort,\nSaleRepositoryPort, SalesReportingPort"]
    end

%% Driving adapters call driving ports
    subgraph Driving["Driving Adapters (inbound)"]
        HTTP["HTTP / Web\nAuthorHttpHandler, RoyaltyHttpHandler"]
        CLI["CLI / Scheduler / MQ Consumers"]
    end

%% Driven adapters implement driven ports
    subgraph Driven["Driven Adapters (outbound)"]
        JPA["JPA / DB\nAuthorRepositoryAdapter, BookRepositoryAdapter,\nSaleRepositoryAdapter, SalesReportingAdapter"]
        MQ["Message Broker Adapter"]
        EXT["External APIs Adapter"]
    end

%% Direction of dependencies/calls
    HTTP --> IP
    CLI  --> IP
    IP   --> UC
    UC   --> OP
    OP   --> JPA
    OP   --> MQ
    OP   --> EXT
```

**20,000 feet overview**
```mermaid
flowchart LR
  dom[domain]
  cor[core]
  web[web]
  data[data]
  app[application]

  cor --> dom
  web --> dom
  data --> dom
  app --> cor
  app --> web
  app --> data
```

---

## 5) Testing in Hexagonal architecture

* **ArchUnit tests** enforce: domain/core cannot import Spring/JPA/Web. Web/data cannot depend on Spring Boot.
* **Integration tests** in application module with Testcontainers + DBUnit.
* **Unit tests** run fast in core/web/data modules with mocks.

### 5.1) What to test in each module
* domain (unit tests): validate domain stays independent of any frameworks.
* core (unit tests): business logic validation and port orchestration.
* web (unit tests): DTO↔domain & error mapping.
* data (unit tests): entity↔domain mappers.
* application (integration tests): Spring Boot + Testcontainers Postgres + Liquibase; hit real HTTP routes and DB.

---

## 6) When to choose Hexagonal

Great fit when:
* The system is long-lived
* The system is non-trivial

Long-lived and non-trivial services need to be ready to anticipate changes to underlying frameworks and other technology.

When a service is small or you expect to retire a service within the current hype-cycle then maybe Hexagonal is overkill.
In such cases a traditional layered architecture could be the better approach. 

### 6.1) Where Layered Architecture Shows Cracks

Lower-layer changes ripple upward (e.g., repository now returns Spring Data `Page<T>`), forcing upper layers to import 
those types or write glue everywhere.

**Example:**

```java
// Repository layer change introduces Spring Data types
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

interface AuthorJpaRepository {
    Page<AuthorEntity> findByName(String name, Pageable pageable);
}

// Service layer is now forced to depend on Spring Data to expose pagination
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

class AuthorService {
    private final AuthorJpaRepository repo;
    AuthorService(AuthorJpaRepository repo) { this.repo = repo; }

    Page<AuthorEntity> search(String name, int page, int size) {
        var pageable = PageRequest.of(page, size);
        return repo.findByName(name, pageable);
    }
}

// Web layer, in turn, ends up returning framework-shaped types
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/authors")
class AuthorController {
    private final AuthorService service;
    AuthorController(AuthorService service) { this.service = service; }

    @GetMapping
    Page<AuthorEntity> search(@RequestParam String name,
                              @RequestParam int page,
                              @RequestParam int size) {
        return service.search(name, page, size);
    }
}
```

* **Consequence:** A persistence-layer decision (Spring Data pagination) ripples up into service and web signatures, increasing blast radius for change.


---

```mermaid
flowchart TB
%% Feature artifact declares logic
  subgraph Feature_Artifact_JAR["Feature Artifact JAR"]
    F1[Declared Streams / Batches]
    F2[Logic: validate / transform / aggregate]
  end

%% Platform (conceptual only)
  subgraph Platform["Platform"]
    P0[Injects implementations and manages connections]
  end

%% External services
  subgraph External_Services["External Services"]
    K[(Kafka)]
    DB[(Database)]
    S3[(S3 / Object Store)]
    REF[(Reference Data API)]
  end

%% Setup: platform injects and manages connections
  P0 -->|inject implementations| Feature_Artifact_JAR
  P0 -.->|manage connections| K
  P0 -.->|manage connections| DB
  P0 -.->|manage connections| S3
  P0 -.->|manage connections| REF
%% Data paths after injection (platform not on hot path)
  K -->|records in| F1
  F1 --> F2
  F2 -->|records out| K
  DB -->|rows| F2
  F2 -->|writes| DB
  F2 -->|files| S3
  F2 -->|lookup| REF
  REF -->|reference data| F2
```

```mermaid
flowchart LR

%% Feature artifacts (business logic only)
  subgraph Feature_Artifacts
    subgraph JAR_A["Feature Artifact JAR A"]
      F1A[Declared Streams / Batches]
      F2A[Logic: validate / transform / aggregate]
      F1A --> F2A
    end
    subgraph JAR_B["Feature Artifact JAR B"]
      F1B[Declared Streams / Batches]
      F2B[Logic: validate / transform / aggregate]
      F1B --> F2B
    end
    subgraph JAR_C["Feature Artifact JAR C"]
      F1C[Declared Streams / Batches]
      F2C[Logic: validate / transform / aggregate]
      F1C --> F2C
    end
  end

%% Platform runtime
  subgraph Platform_Runtime["Platform Runtime"]
    P0[Injects Connections]
  end

%% Operational planes (owned by platform)
  subgraph Platform_Operations["Platform Operations"]
    MON[Monitoring & Observability]
    DEP[Deployments & Lifecycle Mgmt\n+ Security Controls]
  end

%% Managed infrastructure hub
  subgraph External_Services["External Services"]
    direction LR
    K[(Kafka)]
    DB[(Database)]
    S3[(S3 / Object Store)]
    REF[(Reference Data API)]
  end

%% Relationships
  P0 -->|inject runtime connections| JAR_A
  P0 -->|inject runtime connections| JAR_B
  P0 -->|inject runtime connections| JAR_C
  JAR_A --> External_Services
  JAR_B --> External_Services
  JAR_C --> External_Services
%% Operational layers apply to platform, not feature logic
  MON --> Platform_Runtime
  DEP --> Platform_Runtime
  Platform_Runtime -->|Manages connections| External_Services
```

```mermaid
flowchart LR
  %% Layout: stack wide feature boxes, each showing repeated aspects inside
  subgraph Feature_C["Feature C (standalone app)"]
    direction TB
    A_logic[Business Logic]
    subgraph A_repeat["Repeated Implementation per Feature"]
      direction TB
      A_k[Kafka Conn]
      A_db[Database]
      A_s3[S3]
      A_ref[Ref API]
      A_dep[Deploy/LCM]
      A_sec[Security]
      A_mon[Monitoring]
    end
  end

  subgraph Feature_B["Feature B (standalone app)"]
    direction TB
    B_logic[Business Logic]
    subgraph B_repeat["Repeated Implementation per Feature"]
      direction TB
      B_k[Kafka Conn]
      B_db[Database]
      B_s3[S3]
      B_ref[Ref API]
      B_dep[Deploy/LCM]
      B_sec[Security]
      B_mon[Monitoring]
    end
  end

  subgraph Feature_A["Feature A (standalone app)"]
    direction TB
    C_logic[Business Logic]
    subgraph C_repeat["Repeated Implementation per Feature"]
      direction TB
      C_k[Kafka Conn]
      C_db[Database]
      C_s3[S3]
      C_ref[Ref API]
      C_dep[Deploy/LCM]
      C_sec[Security]
      C_mon[Monitoring]
    end
  end
```

```mermaid
sequenceDiagram
  autonumber
  participant OP as Operator
  participant CFG as YAML EngineConfig
  participant L as Engine Loader
  participant B as Binder (transport adapter)
  participant ART as Feature Artifact
  participant C as KafkaConsumer
  participant P as KafkaProducer
  participant K as Kafka Cluster
  participant DB as Database
%% 1) Startup and configuration
  OP ->> L: start(engineId)
  L ->> CFG: load & validate
%% 2) Assemble properties (no clients yet)
  L ->> B: assemble consumer properties (incoming)
  L ->> B: assemble producer properties (outgoing)
  L ->> B: assemble database properties (url, driver, creds)
%% 3) Instantiate artifact (declares workflow using SDK interfaces)
  L ->> ART: instantiate(artifactClass)
%% 4) Create clients and inject into artifact
  B ->> C: create KafkaConsumer(props)
  C ->> K: subscribe(topic-in)
  B ->> P: create KafkaProducer(props)
  B ->> ART: inject Kafka implementations (consumer, producer)
  B ->> ART: inject database connection
  Note over ART: Artifact defines the workflow on SDK interfaces\nPlatform has injected Kafka and DB implementations
%% 5) Stream flows (artifact orchestrates)
  K -->> C: records (stream)
  C -->> ART: elements() provided to artifact
  ART -->> ART: validate / transform / aggregate
  ART -->> DB: insert/update per event
  ART -->> P: emit() transformed events
  P -->> K: produce records to topic-out
```

```mermaid
sequenceDiagram
    autonumber
    participant OP as Operator
    participant CFG as YAML EngineConfig
    participant L as Engine Loader
    participant B as Binder (transport adapter)
    participant ART as Feature Artifact
    participant DB as Database
    participant S3 as Amazon S3
%% 1) Startup and configuration
    OP ->> L: start(engineId)
    L ->> CFG: load & validate
%% 2) Assemble properties and batch execution (no clients yet)
    L ->> B: assemble database properties (url, driver, creds, query template)
    L ->> B: assemble S3 properties (bucket, path template, format, compression)
    L ->> B: resolve schedule -> BatchExecution (window, params)
%% 3) Instantiate artifact (declares workflow using batch SDK)
    L ->> ART: instantiate(artifactClass)
%% 4) Inject connections/clients into the artifact
    B ->> ART: inject database connection and S3 client\nplus BatchExecution context
    Note over ART: Artifact uses IncomingBatch.read() and OutgoingBatch.write()\nPlatform has injected DB and S3 connections
%% 5) Bounded batch flow (platform not on hot path)
    B -->> ART: Trigger batch
    DB -->> ART: supply rows
    ART -->> ART: validate / transform / aggregate
    ART -->> S3: write transformed data
```


```mermaid
classDiagram
    direction LR

    class IncomingStream~T~ {
      +elements() Flow~T~
    }
    class OutgoingStream~T~ {
      +emit(values: Flow~T~) Result~Long~
    }
    class IncomingBatch~T~ {
      +read(exec: BatchExecution) Flow~T~
    }
    class OutgoingBatch~T~ {
      +write(exec: BatchExecution, values: Flow~T~) Result~Long~
    }

    class BatchExecution {
      +id: String
      +windowStartEpochMs: Long
      +windowEndEpochMs: Long
    }

    class EngineConfig {
      +engineId: String
      +artifactClass: String
      +streams: StreamsSection
      +batches: BatchesSection
      +profiles: Profiles
    }

    class StreamsSection {
      +incoming: List~IncomingStreamCfg~
      +outgoing: List~OutgoingStreamCfg~
    }

    class IncomingStreamCfg {
      +name: String
      +bindTo: String
      +kafka: KafkaIncomingBinding
    }

    class OutgoingStreamCfg {
      +name: String
      +kafka: KafkaOutgoingBinding
    }

    class Profiles {
      +kafka: Map~String, KafkaProfile~
    }

    class KafkaIncomingBinding {
      +topic: String
      +profile: String
      +propsOverride: Map~String, Any~
    }
    class KafkaOutgoingBinding {
      +topic: String
      +profile: String
      +propsOverride: Map~String, Any~
    }
    class KafkaProfile {
      +common: Map~String, Any~
      +consumer: Map~String, Any~
      +producer: Map~String, Any~
    }

    EngineConfig --> StreamsSection
    EngineConfig --> Profiles
    StreamsSection --> IncomingStreamCfg
    StreamsSection --> OutgoingStreamCfg
    IncomingStreamCfg --> KafkaIncomingBinding
    OutgoingStreamCfg --> KafkaOutgoingBinding
    Profiles --> KafkaProfile
```

```mermaid
flowchart TB
  subgraph Platform["Platform (stable)"]
    Ld[Engine Loader]
    Or[Orchestrator]
    Bd[Binder]
    Rt[Runtime Pipes]
  end

  subgraph Artifact_v1["Feature Artifact v1"]
    A1[IncomingStream<T> v1]
    B1[OutgoingStream<U> v1]
  end

  subgraph Artifact_v2["Feature Artifact v2"]
    A2[IncomingStream<T> v2]
    B2[OutgoingStream<U> v2]
  end

  Ld --> Rt
  Rt --> Bd
  Bd --- K[(Kafka)]
  Bd --- SR[(Schema Registry)]
  Rt --- A1
  Rt --- B1
  Or -. redeploy (pom bump ) .-> Ld
Ld --> Rt
Rt --- A2
Rt --- B2
```

```mermaid
flowchart LR
  %% Layout
  %% Left-to-right emphasizes inbound sources -> platform -> outbound consumers

  %% ===== External Sources / Sinks =====
  subgraph EXT["External Systems"]
    direction TB
    S1[/"Data Sources\n(XML, CSV, APIs)"/]:::external
    ST[/"Static Data\n(Reference Tables)"/]:::external
    REP[/"Reporting &\nDownstream Consumers"/]:::external
  end

  %% ===== Platform (Domain Agnostic) =====
  subgraph PLAT["Domain-Agnostic Platform (Hexagonal Ports & Adapters)"]
    direction TB

    subgraph PORTS_IN["Inbound Ports / Adapters"]
      ING["Ingestion Adapter\n• Parse & Normalize\n• Schema-Tolerant"]:::platform
      SDS["Static Data Service Adapter\n• Cached Lookups\n• Version-Agnostic"]:::platform
    end

    subgraph CORE["Core Application"]
      RTE["Rule Execution Engine\n• Orchestrates Rule Sets\n• Collects Validation Events"]:::platform
      EVT["Event Recorder\n• Persist Validation Events"]:::platform
      PERS["Persistence Adapter\n• Validated Data Storage"]:::platform
    end

    subgraph PORTS_OUT["Outbound Ports / Adapters"]
      OUT1["Validated Data Export"]:::platform
      OUT2["Events/Notifications"]:::platform
    end
  end

  %% ===== Feature Team Space (Domain Specific) =====
  subgraph FEAT["Feature Team (Domain-Specific DQ Rules)"]
    direction TB
    RULES["DQ Rules (per domain)\n• Business Logic\n• Domain Semantics"]:::feature
  end

  %% ===== SDK (The Glue) =====
  SDK["Rules SDK\n• Contracts & Interfaces\n• Test Harness\n• Type-Safe APIs"]:::sdk

  %% ===== Flows =====
  %% Data in
  S1 -->|"Generic Data Files"| ING
  ST -->|"Reference Lookups"| SDS

  %% Ingestion path
  ING -->|"Normalized Records"| RTE
  SDS -->|"Reference Values"| RTE

  %% Rules invocation via SDK
  RTE <-->|"Invoke Rules\n(ports) via SDK"| SDK
  SDK <-->|"Implements Contracts"| RULES

  %% Outcomes
  RTE -->|"Validation Events"| EVT
  RTE -->|"Validated Entities"| PERS

  %% Outbound publishing
  PERS -->|"Validated Data"| OUT1
  EVT -->|"Events"| OUT2

  %% External consumption
  OUT1 --> REP
  OUT2 --> REP

  %% Styling
  classDef platform fill:#0b6,stroke:#064,stroke-width:1.5,color:#fff;
  classDef feature fill:#06c,stroke:#035,stroke-width:1.5,color:#fff;
  classDef sdk fill:#f80,stroke:#b60,stroke-width:1.5,color:#fff;
  classDef external fill:#bbb,stroke:#666,stroke-width:1.5,color:#111;

  %% Emphasize hexagonal idea with grouped ports/adapters around core
  %% (Mermaid doesn't draw literal hexagons; ports/adapters subgraphs represent the hex edges.)
```
