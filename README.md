# scraping-project-template
Welcome to the Personalization Project repository! T
## About the Template
Web scraping is a critical aspect of our data gathering efforts, and with numerous social media platforms and websites to explore, it's essential to have a structured and organized approach. This template serves as a starting point for your scraping projects, providing essential components and best practices that we've refined through experience.

## Getting started
To create a new scraping project using this template, follow these simple steps:
1. Log in to your GitHub account.
2. Go to the organization's GitHub page.
3. Click on the green "New" button on the right side to create a new repository for your scraping project.
4. Enter required fields and click on "Create repository."
5. Once you created the new repository for your project clone the repository to your local machine using Git:
   ```shell
    git clone https://github.com/Influencers-Club/<Your project repository>
   ```
6. Next, clone the Scraping Project Template repository to your local machine using Git:
   ```shell
    git clone https://github.com/Influencers-Club/scraping-project-template.git
   ```
7. Copy the contents of the scraping project template repository to your project repository:
   ```shell
    cp -r path/to/Scraping-Project-Template/* path/to/your-new-project-repository/
   ```
8. Now you can modify your project repository to suit the unique requirements of your project within the organization.

## Deployment

1. Navigate to cmder or terminal and change directory to your project
2. Build and start containers
   ```shell
   docker-compose --env-file ./env/.env.prod -f docker-compose-prod.yaml up --build -d --force-recreate
   ```

## Full Documentation
https://influencersclub.atlassian.net/l/cp/JQbxxs0L