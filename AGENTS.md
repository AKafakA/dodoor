# Repository Guidelines

## Project Structure & Module Organization
- Java sources: `src/main/java/edu/cam/dodoor/...`
- Generated Thrift: `src/main/gen-java` (do not edit by hand)
- Resources: `src/main/resources` (e.g., `log4j2.xml`)
- Thrift IDL: `Thrift/src/*.thrift`
- Simulator + analysis: `simulator/`, `deploy/python/analysis/`
- Configs: `deploy/resources/configuration/` (e.g., `example_dodoor_configuration.conf`, `host_config_template.json`, `function_bench_config.json`)
- Scripts: `deploy/script/` (experiments, setup, plotting)

## Build, Test, and Development Commands
- Full rebuild (generate Thrift + build jar): `./rebuild.sh`
- Maven build (skip tests): `mvn clean package -DskipTests`
- Run all services via unified daemon:
  `java -cp target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c deploy/resources/configuration/example_dodoor_configuration.conf -hc deploy/resources/configuration/host_config_template.json -tc deploy/resources/configuration/function_bench_config.json -s true -d true -n true`
- Experiments: `./deploy/script/single_exp.sh` or `./deploy/script/end_to_end_exp/debug.sh`
- Simulator (Python): `PYTHONPATH=. python simulator/run_simulation.py --config simulator/config/debug_config.json`

## Coding Style & Naming Conventions
- Java 17, Maven. Indent with 4 spaces. Packages under `edu.cam.dodoor.*`.
- Use SLF4J (`LoggerFactory.getLogger`) for logs; avoid `System.out`.
- Classes: UpperCamelCase; methods/fields: lowerCamelCase; constants: `UPPER_SNAKE_CASE`.
- Keep changes minimal; prefer existing utils in `utils/`. No new deps without justification.
- Python simulator: follow PEP 8; add type hints for new modules.

## Testing Guidelines
- No unit test suite by default. Validate changes with:
  - End‑to‑end scripts under `deploy/script/`
  - Simulator runs (`simulator/`) and existing plotting tools
- If adding tests: JUnit 4 under `src/test/java`, name files `*Test.java`, run `mvn test`.

## Commit & Pull Request Guidelines
- Commits: short, imperative, scoped prefixes encouraged (e.g., `scheduler: fix batching`, `simulator: add plots`, `docs:`).
- PRs include: concise summary, rationale, relevant configs changed, reproduction commands, and key logs/plots. Update docs if scripts/configs change.
- Do not commit generated code (`src/main/gen-java`), logs (`application-*.log`), or build outputs (`target/`).

## Security & Configuration Tips
- Thrift changes: edit `Thrift/src/*.thrift` and run `./rebuild.sh`; ensure Java and simulator remain aligned.
- Ports, thread counts, and metrics toggles live in `deploy/resources/configuration/*.conf/json` and `DodoorConf` defaults—document overrides in PRs.

## Agent-Specific Notes
- When modifying scheduling logic, update both Java (`scheduler/*`) and simulator implementations to keep behavior aligned.
- Favor surgical diffs; avoid renames that break scripts in `deploy/script/`.
