# DBUpdater Service

A market-analysis flask service for fetching and caching data from various APIs.

## Table of Contents
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Reset](#reset)
- [Contributing](#contributing)
- [License](#license)

## Installation

### Prerequisites

Before you begin, ensure you have the following installed on your system:

- **Git**: [Install Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
- **Helm**: Install via the following commands:
  
  ```sh
  sudo apt update
  sudo apt install helm
  ```
  
- **Minikube**: Install via the following commands:
  
  ```sh
  sudo apt update
  sudo apt install minikube
  minikube start
  ```
  
  **Clone the repository**: Navigate to your chosen install directory and clone the repo.
  
  ```sh
  cd path/to/install/directory
  git clone https://github.com/Tacos4brekky2/market-analysis_server.git
  cd market-analysis_server
  ```

### SETUP

Before you try to set container secrets using one of these methods, ensure you set
environment variables listed in set_secrets.sh (This is temporary)

  **WIP-Set container secrets**:
  
  Use the provided script in tools/ or enter the following commands:
  
  ```sh
  bash tools/set_secrets.sh
  ```

  Alternatively, you can copy and paste the commands directly.

  ```sh
  kubectl create secret generic mongo-secret \
      --from-literal=host=$STONKS_MONGO_HOST \
      --from-literal=port=$STONKS_MONGO_PORT \
      --from-literal=user=$STONKS_MONGO_USER \
      --from-literal=password=$STONKS_MONGO_PASSWORD \
      --from-literal=dbname=$STONKS_MONGO_DB_NAME
  kubectl create secret generic redis-secret \
      --from-literal=host=$STONKS_REDIS_HOST \
      --from-literal=port=$STONKS_REDIS_PORT \
      --from-literal=password=$STONKS_REDIS_PASSWORD
  kubectl create secret generic keys-secret \
      --from-literal=rapidapi=$STONKS_KEY_RAPIDAPI
  ```
  
  **Fetch helm dependencies**:

  Fetch chart dependencies from the Chart.yaml file:

  ```sh
  cd helm
  helm dependency update
  ```

## DEPLOYMENT

  Install the market-analysis helm chart and deploy the app:

  ```sh
  helm install market-analysis .
  cd ..
  ```

## RESET

  To delete everything created in the installation process, run the reset script in tools:

  ```sh
  bash tools/reset.sh
  ```

  Again, you can instead copy and paste the commands directly:

  ```sh
  helm uninstall market-analysis
  kubectl delete pvc mongodb-pvc
  kubectl delete pv mongodb-pv
  kubectl delete pvc redis-master-pvc
  kubectl delete pvc redis-master-pv
  kubectl delete pvc redis-slave-pvc
  kubectl delete pvc redis-slave-pv
  kubectl delete secret mongo-secret
  kubectl delete secret redis-secret
  kubectl delete secret keys-secret
  ```
  
## CONTRIBUTING
## LICENSE










