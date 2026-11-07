# Lab — Apache Polaris Deployment

## Objectif

Deployer Apache Polaris 1.8.0 sur Kubernetes avec Helm et PostgreSQL.

## Technologies

- Apache Polaris 1.8.0
- Kubernetes et Helm
- PostgreSQL
- Apache Iceberg
- Stockage S3

## Fichiers

- deployment/Chart.yaml : dependances Helm
- deployment/values.yaml : configuration
- deployment/templates/bootstrap-job.yaml : initialisation du catalogue

## Deploiement

Commande Helm :

    helm dependency update polaris/deployment
    helm lint polaris/deployment
    helm upgrade --install polaris polaris/deployment --wait --timeout 8m

## Verification

    kubectl get pods
    kubectl get jobs
    helm status polaris
    curl -fsS http://polaris-mgmt:8182/q/health/ready

## Resultats

- Polaris : Running
- PostgreSQL : Running
- Bootstrap Job : Complete
- API Polaris : UP
- Helm : deployed (revision 2)

## Securite

Les identifiants PostgreSQL, Polaris et S3 sont stockes dans
des Secrets Kubernetes et ne sont pas inclus dans ce depot.

Ne pas supprimer le deploiement : Polaris sera reutilise
pour le prochain TP.
