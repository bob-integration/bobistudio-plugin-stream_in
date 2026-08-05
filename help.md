# Stream In — source réseau SRT/RTMP

Ingère un flux réseau (contribution, OBS, encodeur distant…) et le publie sur le bus MXL
comme n'importe quelle source : câblable vers un multiview, un mélangeur, une sortie 2110…

## Modes d'ingest

- **SRT — écoute** : l'instance écoute sur son port ; la source distante pousse en mode
  *caller* vers l'URL affichée dans l'UI (`srt://<ip>:<port>?mode=caller`).
- **SRT — appel** : l'instance se connecte à une URL SRT distante (la source est en *listener*).
- **RTMP — serveur** : l'instance écoute en `rtmp://` ; une source type OBS pousse vers
  `rtmp://<ip>:<port>/<application>/<clé>`. Un seul publieur à la fois.
- **RTMP — pull** : l'instance tire un flux `rtmp://` existant.

L'IP affichée est celle du **conteneur** (macvlan) : elle est directement joignable sur le
LAN, sans redirection de port. Le port d'écoute est libre — vérifier qu'il ne collisionne
pas avec une autre instance sur le même nœud.

**Passphrase / Stream ID** (modes SRT) : la passphrase chiffre le transport (AES, à
renseigner identique des deux côtés) ; le Stream ID sert au routage/authentification côté
certains encodeurs ou plateformes (Haivision, Wowza…) qui l'exigent pour accepter la
connexion. Les deux sont facultatifs et s'appliquent aux deux modes SRT (écoute et appel).

## Point d'entrée

L'adresse affichée dans le panneau **est** l'adresse réglable : le port (modes écoute), les
segments application et clé (RTMP serveur) ou l'URL distante (modes appel et pull) s'éditent
directement là où ils sont lus. Appliquer relance la connexion sans redéployer, la sortie MXL
n'est pas coupée. Les huit dernières adresses utilisées restent accessibles en un clic.

L'**interrupteur Ingest** coupe ffmpeg sans arrêter le conteneur : la sortie continue d'émettre
la mire à la cadence configurée, les consommateurs câblés ne voient aucune rupture.

## Journal de connexion

Chaque transition d'état, redémarrage de ffmpeg, erreur, changement de source et bascule
marche/arrêt est horodaté dans un journal persistant, qui survit au redémarrage du script.
Consultable par plage date/heure et exportable en CSV : c'est la pièce justificative quand il
faut établir d'où venait une coupure. Les horodatages viennent du conteneur, dont l'horloge est
disciplinée par PTP, donc comparables avec le reste de la chaîne. Vider le journal est possible,
demande confirmation, et l'effacement est lui-même consigné.

## Format de sortie (conform)

Le format de sortie se choisit au déploiement, dans le sélecteur de format de la palette,
alimenté par Réglages → Formats vidéo. Le panneau affiche côte à côte le format **réel** de la
source (avant conform) et le format de sortie.

Le flux entrant est **toujours reconverti** au format configuré (résolution, cadence, chroma,
profondeur, balayage) : scale bicubique, conversion de cadence, désentrelacement automatique
si l'entrée est entrelacée. La sortie MXL est donc un signal broadcast propre et stable quel
que soit ce qu'envoie la source. Pour une sortie entrelacée (ex. 1080i50) : cadence = cadence
**trame** (25) et balayage = entrelacé.

## Genlock

**Genlock (grille PTP)** (activé par défaut) : l'image de sortie est écrite sur un index
calé sur la grille PTP rationnelle — une trame par tick, comme un producteur normal du
pipeline, alignée avec les autres sources pour un mixage/multiview cohérent. Désactivé, la
sortie suit le rythme d'arrivée de la source (index libre) : à réserver aux cas où l'aval ne
dépend d'aucune synchro de grille (rare — ne pas désactiver sans raison précise).

## États

- **En attente de flux** : aucune image reçue — l'instance émet la mire (ou du noir, réglage
  « Image sans flux »).
- **Connecté** : images fraîches, sortie conformée en direct.
- **Gelé** : plus d'image depuis le seuil configuré — la dernière image est répétée (le signal
  MXL ne s'interrompt jamais). Reprise automatique dès que le flux revient ; en modes
  appel/pull, une connexion zombie est tuée et retentée automatiquement.

## Audio

L'audio du flux est décodé vers un shm 8 canaux 48 kHz (câblable page Câbles). Sans piste
audio dans le flux, l'instance bascule automatiquement en vidéo seule (silence sur le shm).

## Diagnostic audio

`:8080` expose la santé du chemin audio — en régime établi, les trois compteurs doivent rester
figés :

- `audio_buf_ms` : occupation du tampon = **latence de l'audio par rapport à la vidéo**
  (40–60 ms attendus). Une valeur qui monte et reste haute = audio en retard.
- `audio_starve_blocks` : blocs de silence insérés faute d'audio frais (s'incrémente pendant
  l'attente d'une source, c'est normal).
- `audio_drop_samples` : échantillons jetés pour ramener la latence à la cible (une salve au
  démarrage est normale).
- `audio_reanchors` : ré-ancrages sur la grille TAI (dérive > 250 ms de l'horloge source).
- `audio_in_sps` : échantillons/s livrés par ffmpeg (≈ 48000).

Le flux audio est écrit sur le bus par blocs de **1 ms** : c'est le contrat attendu par les
consommateurs (le streamer lit par blocs de 1 ms et comble au silence ce qui manque). Ne pas
augmenter cette granularité — l'audio se mettrait à sauter chez les consommateurs.

## Mode tranche (latence réduite)

Paramètre avancé `slice_mode` (désactivé par défaut, comportement inchangé) : la sortie
vidéo — déjà décodée/conformée par ffmpeg — est publiée bande par bande sur le bus MXL au
lieu d'attendre l'image complète, pour retirer une image de latence à l'aval (multiview,
mixer, streamer en mode tranche). `slice_lines` fixe la hauteur des bandes et doit diviser
la hauteur de sortie (36 pour 1080p) ; sinon repli automatique en image entière. Une source
entrelacée retombe aussi en image entière. N'affecte ni l'ingest ni l'audio.

## Contrôle

- `GET /state` : état complet (connexion, fps entrée/sortie, âge de la dernière image,
  redémarrages ffmpeg, derniers messages d'erreur).
- `POST /reconnect` : force la coupure et la relance de la connexion (re-tente aussi l'audio).
