N3C Data Ingestion & Harmonization (DI&H) Repository
==================================================

⚠️ ARCHIVAL NOTICE: SNAPSHOT / NOT MAINTAINED

This repository is a static snapshot of the National Clinical Cohort Collaborative (N3C) Data Ingestion and Harmonization code. It is provided for transparency, reproducibility, and historical reference.

This codebase is not actively maintained. Issues submitted regarding bugs, feature requests, or installation support may not be addressed.

Overview
--------

This monorepo consolidates the various scripts, pipelines, and utilities that constitute the backend of the N3C platform's data ingestion and harmonization (DI&H) infrastructure.

The N3C platform ingests clinical data from contributing sites across the country, transforming various Common Data Models (CDMs) such as OMOP, PCORnet, ACT, and TriNetX into a unified, harmonized schema for research. This repository serves as a centralized record of the logic used to achieve that harmonization.

Repository Contents
-------------------

This repository contains code covering the full lifecycle of the ingestion process, including but not limited to:

-   CDM Mapping: Logic for cross-walking source data models (PCORnet, ACT, TriNetX) to the target OMOP CDM.

-   Data Cleaning: Scripts for data quality checks and sanitation.

-   Harmonization: The core logic used to normalize units, concepts, and domains across disparate datasets.

-   Ingestion Pipelines: Orchestration code used to manage the flow of data from submission to release.

-   Utilities: Helper scripts and tools used throughout the DI&H process.

Contributors
------------

The development of the N3C Data Ingestion & Harmonization pipeline was a massive, collaborative effort involving researchers, engineers, and informaticians from numerous institutions.

We have done our best to compile a comprehensive list of all contributors to this codebase over time, listed alphabetically below.

### Contributor List
|                                  |                                   |
| -------------------------------- | --------------------------------- |
| Abhishek Bhatia                  | Lisa Eskenazi                     |
| Adam Lee                         | Margaret Hall                     |
| Alfons von Rosty-Forgách         | Mariam Deacy                      |
| Amin Manna                       | Mark Bissell                      |
| Andrea Zhou                      | Matthew Owens                     |
| Andrew Girvin                    | Matthew Pagel                     |
| Andrew Laitman                   | Matthew Steele                    |
| Anitej Biradar                   | Maya Choudhury                    |
| Anna O'Malley                    | Clair Blacketer                   |
| Bengt Ljungquist                 | Melissa Haendel                   |
| Benjamin Amor                    | Mengshuo Ye                       |
| Benjamin Zook                    | Michele Morris                    |
| Briana Abraham                   | Mika Jugovich                     |
| Bruno Rahle                      | Muhammad Emir Amaro Syailendra    |
| Bryan Laraway                    | Nabeel Qureshi                    |
| Chris Roeder                     | Nathan Turlington                 |
| Christopher Chute                | Nick Schaub                       |
| Claire Draeger                   | Peter Leese                       |
| Daniel Yeomans                   | Philip Sparks                    |
| Danny Puller                     | Poorna Bharanikumar               |
| Davera Gabriel                   | Pradeep Bandaru                   |
| Emily Niehaus                    | Rhea Bhakhri                      |
| Emily Pfaff                      | Richard Moffitt                   |
| Emily Clark                      | Richard Zhu                       |
| Gary Clark                       | Rish Jain                         |
| Gianna Beck                      | Robert Miller                     |
| Guillaume Soulé                  | Saad Ljazouli                     |
| Hadrien Maupard                  | Sai Mada                          |
| Harish Ramadas                   | Shijia Zhang                      |
| Harold Lehmann                   | Sigfried Gold                     |
| Heidi Spratt                     | Sofia Dard                        |
| Hythem Sidky                     | Stephanie Hong                    |
| Hyun Woo Kim                     | Steve Makkar                      |
| James Cavallon                   | Tanner Zhang                      |
| James Evans                      | Thomas Shouler                    |
| Janos Hajagos                    | Tim Schwab                        |
| Johanna Loomba                   | Timothy Bergquist                 |
| Joseph Kane                     | Xiaohan Zhang                     |
| Kanchan Chaudhari                | Yi-Ju Chen                        |
| Kate Bradwell                    | Yun Jae Yoo                       |
| Kate Sanders                     | Yurii Mashtalir                   |
| Kellie Walters                   |                                   |
| Kelly Jones                      |                                   |
| Kenneth Wilkins                  |                                   |
| Kristen Hansen                   |                                   |
| Kristin Kostka                   |                                   |

### Missing or Incorrect Information?

While this repository is not maintained for code updates, we want to ensure proper credit is given.

If you contributed to this code and your name is missing, or if you would like your attribution updated (e.g., affiliation change, name change), please open an issue in this repository with the tag attribution. We will do our best to update the documentation to reflect your contributions accurately.

Disclaimer
----------

This software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and non-infringement.

This code represents a snapshot of the N3C infrastructure at a specific point in time. As the platform evolves, the production code may diverge from what is hosted here. This repository should be viewed as a scientific artifact supporting the transparency of the N3C data harmonization process.

License
-------

See LICENSE file for details.
