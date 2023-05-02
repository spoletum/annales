job "annales" {
  datacenters = ["dc1"]
  type = "service"

  group "mongodb" {
    network {
      mode = "bridge"
      port "mongodb" {
        static = 27017
      }
    }

    task "mongo" {
      driver = "docker"
      config {
        image = "mongo:latest"
      }
      env {
        MONGO_INITDB_ROOT_USERNAME = "admin"
        MONGO_INITDB_ROOT_PASSWORD = "password"
      }
      resources {
        cpu    = 500
        memory = 512
      }
    }
    service {
      name = "mongodb"
      port = "mongodb"
    }    
  }
}
