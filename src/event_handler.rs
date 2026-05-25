use lambda_runtime::{Error, LambdaEvent};
use aws_lambda_events::event::eventbridge::EventBridgeEvent;
use std::{collections::HashMap, env};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT, ACCEPT};
use serde::{Serialize, Deserialize};
use serde_json::json;

// El payload que espera recibir tu Lambda
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CustomEvent {
    s3_role_name: String,
    ecs_role_name: String,
    recipes_bucket_name: String,
    host_bucket_name: String,
    users_task_family_name: String,
    recipes_task_family_name: String,
    recipes_service: String,
    users_service: String,
}

#[derive(Serialize)]
struct WorkflowDispatchBody {
    r#ref: String,
}

// Definimos un alias de tipo para que el código sea más legible
type RepoEnvMap = HashMap<String, HashMap<String, String>>;

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
pub(crate)async fn function_handler(event: LambdaEvent<EventBridgeEvent<CustomEvent>>) -> Result<serde_json::Value, Error> {
    // Extraemos nuestro detalle del evento de EventBridge

    let owner = "vijote";
    let repos = vec!["users-ms-a874948da", "recipes-ms-a874948da", "recipes-mf-a874948da", "host-mf-a874948da"];

    // 1. Recuperar token de entorno
    let github_token = env::var("GITHUB_TOKEN")
        .map_err(|_| Error::from("Falta la variable de entorno GITHUB_TOKEN"))?;

    // 2. Configurar el cliente de reqwest
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {}", github_token))?);
    headers.insert(ACCEPT, HeaderValue::from_static("application/vnd.github+json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("aws-lambda-eventbridge-rust"));
    headers.insert("X-GitHub-Api-Version", HeaderValue::from_static("2026-03-10"));

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;

    // 3. Lanzar ejecuciones concurrentes por cada repositorio
    let mut tareas = vec![];

    let mut github_updates: RepoEnvMap = HashMap::new();

    let mut recipes_mf_vars = HashMap::new();
    recipes_mf_vars.insert("AWS_GITHUB_S3_ROLE_NAME".to_string(), event.payload.detail.s3_role_name.clone());
    recipes_mf_vars.insert("AWS_BUCKET_NAME".to_string(), event.payload.detail.recipes_bucket_name.clone());
    github_updates.insert("recipes-mf-a874948da".to_string(), recipes_mf_vars);

    let mut host_mf_vars = HashMap::new();
    host_mf_vars.insert("AWS_GITHUB_S3_ROLE_NAME".to_string(), event.payload.detail.s3_role_name.clone());
    host_mf_vars.insert("AWS_BUCKET_NAME".to_string(), event.payload.detail.host_bucket_name.clone());
    github_updates.insert("host-mf-a874948da".to_string(), host_mf_vars);

    let mut users_ms_vars = HashMap::new();
    users_ms_vars.insert("AWS_GITHUB_ECS_ROLE_NAME".to_string(), event.payload.detail.ecs_role_name.clone());
    users_ms_vars.insert("AWS_TASK_FAMILY_NAME".to_string(), event.payload.detail.users_task_family_name.clone());
    users_ms_vars.insert("ECS_SERVICE".to_string(), event.payload.detail.users_service.clone());
    github_updates.insert("users-ms-a874948da".to_string(), users_ms_vars);

    let mut recipes_ms_vars = HashMap::new();
    recipes_ms_vars.insert("AWS_GITHUB_ECS_ROLE_NAME".to_string(), event.payload.detail.ecs_role_name.clone());
    recipes_ms_vars.insert("AWS_TASK_FAMILY_NAME".to_string(), event.payload.detail.recipes_task_family_name.clone());
    recipes_ms_vars.insert("ECS_SERVICE".to_string(), event.payload.detail.recipes_service.clone());
    github_updates.insert("recipes-ms-a874948da".to_string(), recipes_ms_vars);


    for repo in repos {
        let client_clone = client.clone();
        let github_variables_clone = github_updates.clone();

        let tarea = tokio::spawn(async move {
            process_repo(client_clone, owner, repo, github_variables_clone).await
        });
        tareas.push(tarea);
    }

    // 4. Esperar resultados y recolectar errores
    let mut errores = vec![];
    for tarea in tareas {
        match tarea.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => errores.push(format!("Error en GitHub API: {}", e)),
            Err(e) => errores.push(format!("Error crítico de Tokio Join: {}", e)),
        }
    }

    // Si hubo errores, lanzamos el Error para que EventBridge sepa que falló 
    // y aplique las políticas de reintento nativas (o mande a una DLQ si está configurada)
    if !errores.is_empty() {
        return Err(Error::from(format!("Fallaron algunas integraciones: {:?}", errores)));
    }

    Ok(serde_json::json!({
        "status": "success",
        "processed_repos_count": 4
    }))
}

async fn process_repo(
    client: reqwest::Client,
    owner: &str,
    repo: &str,
    github_variable_map: RepoEnvMap,
) -> Result<(), Error> {
    for (var_name, var_value) in github_variable_map.get(repo).unwrap() {        
        let variable_update_url = format!("https://api.github.com/repos/{}/{}/actions/variables/{}", owner, repo, var_name);
        let res_var = client.patch(&variable_update_url)
            // struct literal body without path. struct name missing for struct literal
            .json(&json!({ "name": var_name, "value": var_value }))
            .send()
            .await?;

        if !res_var.status().is_success() {
            let err_text = res_var.text().await.unwrap_or_default();
            return Err(Error::from(format!("Fallo al setear variable en {}: {}", repo, err_text)));
        }
    }
    


    // Paso B: Disparar Workflow Dispatch
    let dispatch_url = format!(
        "https://api.github.com/repos/{}/{}/actions/workflows/deploy.yml/dispatches", 
        owner, repo
    );

    let dispatch_body = WorkflowDispatchBody {
        r#ref: "main".to_string(),
    };

    let res_dispatch = client.post(&dispatch_url).json(&dispatch_body).send().await?;
    if !res_dispatch.status().is_success() {
        let err_text = res_dispatch.text().await.unwrap_or_default();
        return Err(Error::from(format!("Error dispatch [{}]: {}", repo, err_text)));
    }

    Ok(())
}