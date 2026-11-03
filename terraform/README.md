# Infraestructura AWS del data warehouse

Un solo stack de Terraform crea todo el ambiente. Un `apply` lo levanta y un
`destroy` lo elimina, sin pasos intermedios entre stacks.

```mermaid
flowchart LR
    subgraph VPC["VPC 10.20.0.0/16"]
      subgraph Pub["Subred pública"]
        EC2["EC2: Airflow + dbt<br/>(Docker Compose)"]
      end
      subgraph Priv["Subredes privadas (3 AZ)"]
        RDS[("RDS MySQL 8.4")]
        DOC[("DocumentDB 5.0")]
        RS[("Redshift Serverless")]
        QSENI["ENI QuickSight"]
      end
      S3EP["Endpoint gateway S3"]
    end
    S3[("S3 privado<br/>raw/ · control/")]
    SM["Secrets Manager"]
    SSM["SSM Parameter Store"]
    QS["QuickSight"]

    EC2 -- 3306 TLS --> RDS
    EC2 -- 27017 TLS --> DOC
    EC2 -- 5439 TLS --> RS
    EC2 -- PutObject --> S3EP --> S3
    RS -- "COPY (rol IAM)" --> S3EP
    EC2 -. GetSecretValue .-> SM
    EC2 -. GetParameter .-> SSM
    QS --> QSENI -- 5439 --> RS
```

## Archivos

| Archivo | Qué crea |
|---|---|
| `network.tf` | VPC, 1 subred pública, 3 privadas, IGW, endpoint gateway de S3 y los 5 security groups con su matriz de conexiones |
| `storage.tf` | Bucket privado: versionado, SSE-S3, bloqueo de acceso público, política que exige TLS y ciclo de vida de versiones antiguas |
| `sources.tf` | RDS MySQL y DocumentDB privados, cifrados, con TLS obligatorio y contraseña gestionada por Secrets Manager |
| `redshift.tf` | Namespace y workgroup de Redshift Serverless privado, con enhanced VPC routing, SSL obligatorio y límite diario de horas-RPU |
| `iam.tf` | Rol de EC2 (S3, secretos, Redshift, SPICE y SSM), rol de COPY para Redshift y rol de la conexión VPC de QuickSight |
| `ec2.tf` | Parámetro SSM con la configuración del pipeline e instancia con IMDSv2 |
| `user_data.sh.tftpl` | Arranque: Docker, clonado del repo, certificado TLS de AWS, `.env` y `docker compose up` |
| `quicksight.tf` | Conexión VPC de QuickSight hacia Redshift (opcional) |
| `budget.tf` | Presupuesto mensual con alertas por correo (opcional) |

## Conexiones permitidas

| Origen | Destino | Puerto | Cómo se autoriza |
|---|---|---|---|
| Airflow (EC2) | RDS MySQL | 3306 | SG `mysql` ← SG `airflow`; usuario y contraseña desde Secrets Manager; TLS obligatorio |
| Airflow (EC2) | DocumentDB | 27017 | SG `docdb` ← SG `airflow`; Secrets Manager; TLS obligatorio |
| Airflow (EC2) / dbt | Redshift | 5439 | SG `redshift` ← SG `airflow`; Secrets Manager o `GetCredentials`; `require_ssl` |
| Airflow (EC2) | S3 `raw/`, `control/` | 443 | Rol `airflow-ec2`; sin `DeleteObject` en `raw/` |
| Redshift | S3 `raw/` | 443 | Rol `redshift-s3` (solo lectura); tráfico por el endpoint gateway |
| QuickSight | Redshift | 5439 | Conexión VPC: SG `quicksight` ↔ SG `redshift` |
| Equipo local | Todo lo anterior | — | Solo por túneles de Session Manager a través de la instancia |

Ninguna base de datos es pública ni tiene ruta a internet. No existen llaves
de acceso: la instancia usa su rol por IMDSv2 y las contraseñas viven en
Secrets Manager. El `.env` de la instancia contiene solo endpoints y ARNs,
más las claves propias de Airflow generadas en la misma instancia.

## Requisitos

- Terraform ≥ 1.5 y AWS CLI v2 con el
  [Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html).
- Un perfil de AWS CLI con permisos de administrador (en los ejemplos, `admin`).
- La rama que se despliega debe estar en GitHub (`repo_branch`), porque la
  instancia la clona por HTTPS. Si el repositorio es privado, hay que
  hacerlo público o cambiar `repo_url`.

## Despliegue

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # ajustar perfil, rama, IP
terraform init
terraform plan
terraform apply                                 # ~15-20 min (DocumentDB y RDS son lo más lento)
```

Después de `apply`, la instancia tarda de 5 a 10 minutos más en construir
la imagen de Airflow:

```bash
eval "$(terraform output -raw ssm_session)"
sudo tail -f /var/log/cloud-init-output.log     # termina en "== Arranque terminado"
dwc ps                                          # atajo de docker compose
```

### QuickSight (segundo apply)

1. Suscribir la cuenta a QuickSight en la consola, en la misma región.
2. `terraform apply -var enable_quicksight_vpc_connection=true`
3. En QuickSight crear el data source de Redshift con la conexión VPC
   `ecommerce-dw-redshift`, el endpoint `terraform output -raw redshift_endpoint`,
   el puerto 5439 y la base `ecommerce_dw`. Se recomienda un usuario de
   solo lectura sobre `marts` en lugar del administrador.

## Acceso

| Para | Comando (desde `terraform/`) |
|---|---|
| Terminal en la instancia | `eval "$(terraform output -raw ssm_session)"` |
| UI de Airflow → `localhost:8080` | `eval "$(terraform output -raw ssm_tunnel_airflow)"` |
| DBeaver → MySQL en `localhost:3307` | `eval "$(terraform output -raw ssm_tunnel_mysql)"` |
| Compass → DocumentDB en `localhost:27018` | `eval "$(terraform output -raw ssm_tunnel_docdb)"` |
| DBeaver → Redshift en `localhost:5439` | `eval "$(terraform output -raw ssm_tunnel_redshift)"` |
| Credenciales de una base | `terraform output secret_value_commands` |
| Contraseña de Airflow (usuario `airflow`) | dentro de la sesión: `terraform output -raw airflow_password_command` |

Por el túnel, DocumentDB necesita `tls=true&tlsCAFile=global-bundle.pem&tlsAllowInvalidHostnames=true&directConnection=true`,
porque el certificado está emitido para el endpoint y no para `localhost`.

Si cambia un endpoint (p. ej. al recrear una base), dentro de la instancia:

```bash
sudo dw-render-env && dwc up -d
```

## Pausar para ahorrar

| Recurso | Pausar | Reanudar |
|---|---|---|
| EC2 | `aws ec2 stop-instances --instance-ids <id>` | `start-instances` |
| RDS | `aws rds stop-db-instance --db-instance-identifier ecommerce-dw-mysql` | `start-db-instance` (AWS lo reinicia solo a los 7 días) |
| DocumentDB | `aws docdb stop-db-cluster --db-cluster-identifier ecommerce-dw-docdb` | `start-db-cluster` (igual, 7 días) |
| Redshift Serverless | Se cobra solo mientras ejecuta consultas | — |

## Eliminar todo

```bash
terraform destroy
```

Borra la VPC, las bases (sin snapshot final), el bucket con su contenido
(`s3_force_destroy = true`), los roles y la instancia. Los secretos
gestionados se eliminan junto con su base. La suscripción de QuickSight es
de la cuenta y se cancela a mano en la consola.

## Costo estimado (us-east-2, on-demand, aproximado)

| Recurso | Precio | 8 h de trabajo | Encendido 24/7 un mes |
|---|---|---|---|
| EC2 `t3.medium` + IPv4 + gp3 30 GiB | ~USD 0,047/h + 2,4/mes | ~0,40 | ~36 |
| RDS `db.t4g.micro` + 20 GiB gp3 | ~USD 0,016/h + 2,3/mes | ~0,15 | ~14 |
| DocumentDB `db.t4g.medium` + almacenamiento e I/O | ~USD 0,08/h | ~0,65 | ~60 |
| Redshift Serverless 8 RPU | ~USD 0,375/RPU-h, por segundo y solo en uso | ~1-3 (según consultas) | Depende del uso; tope diario de 16 RPU-h ≈ USD 6/día |
| Secrets Manager (3 secretos) | USD 0,40/secreto-mes | ~0,01 | 1,2 |
| S3, endpoint gateway, SSM | Céntimos | ~0 | <1 |
| QuickSight (Author) | ~USD 24/mes; 30 días de prueba | — | 24 |

DocumentDB es el recurso fijo más caro, así que conviene detenerlo (o
destruir el stack) al terminar cada sesión. Hay que confirmar los precios
con la [calculadora de AWS](https://calculator.aws/) antes de la entrega.
